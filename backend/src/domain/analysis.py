"""校验交接事件并调用分批回放引擎，计算快照和区间勾稽。"""

from dataclasses import asdict
from datetime import date

from .replay import BATCH_SIZE, Event, Replay, build_result
from .spec import SPEC

_FIELD_BY_KEY = {field["key"]: field for field in SPEC["fields"]}
_PARAM_DEFAULTS = {field["key"]: field["default"] for field in SPEC["params"]}


def _day(value, label):
    try:
        return date.fromisoformat(value)
    except (TypeError, ValueError):
        raise ValueError(f"{label}须为有效日期。") from None


def _event_text(row, key, index, *, required=True):
    field = _FIELD_BY_KEY[key]
    raw = row.get(key)
    if raw is None and not required:
        return None
    if isinstance(raw, str):
        value = raw.strip()
        if not value and not required:
            return None
        if value and len(value) <= field["max_length"]:
            return value
    raise ValueError(f"第 {index} 行的{field['label']}须为 1 至 {field['max_length']} 个字符。")


def _ordered_events(rows):
    if not isinstance(rows, (list, tuple)) or not rows:
        raise ValueError("交接明细为空，请导入至少一条生命周期事件。")
    by_id = {}
    by_slot = {}
    sequence_field = _FIELD_BY_KEY["sequence"]
    for index, row in enumerate(rows, 1):
        if not isinstance(row, dict):
            raise ValueError(f"第 {index} 行交接事件格式有误。")
        kind = row.get("event_type")
        if kind not in _FIELD_BY_KEY["event_type"]["options"]:
            raise ValueError(f"第 {index} 行事件类型无效，须为 accept、check-out、check-in 或 retire。")
        sequence = row.get("sequence")
        if type(sequence) is not int or not sequence_field["min"] <= sequence <= sequence_field["max"]:
            raise ValueError(f"第 {index} 行顺序号须为 1 至 {sequence_field['max']} 的整数。")
        event = Event(
            _event_text(row, "event_id", index), _event_text(row, "asset_id", index),
            _event_text(row, "asset_name", index, required=kind == "accept"),
            _day(row.get("event_date"), f"第 {index} 行交接日期"), sequence, kind,
        )
        previous = by_id.get(event.event_id)
        if previous == event:
            continue
        if previous is not None:
            raise ValueError(f"事件编号 {event.event_id} 的内容冲突，请核对原始交接单。")
        slot = (event.asset_id, event.event_date, event.sequence)
        if slot in by_slot:
            raise ValueError(f"工具 {event.asset_id} 在 {event.event_date} 的顺序号 {sequence} 重复"
                             f"（{by_slot[slot]}、{event.event_id}）。")
        by_slot[slot] = event.event_id
        by_id[event.event_id] = event
    return sorted(by_id.values(), key=lambda event: (event.event_date, event.asset_id, event.sequence))


def validate_rows(rows):
    """校验事件格式与重放键，返回去重后的规范事件；状态由盘点截止日决定。"""
    return [dict(asdict(event), event_date=event.event_date.isoformat()) for event in _ordered_events(rows)]


def analyze(rows, normalized_params):
    """生成日终资产明细、逐月勾稽及整段期初期末。

    与后台大盘一致：校验去重后按（交接日期、工具编号、顺序号）排序，
    以每批 BATCH_SIZE 笔送入同一个回放引擎，口径不因分批而改变。
    """
    if not isinstance(normalized_params, dict) or normalized_params.keys() - _PARAM_DEFAULTS.keys():
        raise ValueError("盘点参数格式有误，请核对统计首日和盘点截止日。")
    params = {**_PARAM_DEFAULTS, **normalized_params}
    start = _day(params["start_date"], "统计首日")
    cutoff = _day(params["as_of_date"], "盘点截止日")

    events = _ordered_events(rows)
    replay = Replay(start, cutoff)
    states = {}
    for offset in range(0, len(events), BATCH_SIZE):
        replay.consume(events[offset:offset + BATCH_SIZE], states)
    replay.close()
    return build_result(replay, sorted(states.items()))
