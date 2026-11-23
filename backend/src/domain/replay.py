"""分批回放交接簿的内存有界引擎。

交接簿可达百万行，站里机器一次最多暂放 BATCH_SIZE 笔。引擎每批消费
不超过 BATCH_SIZE 条已按回放顺序（交接日期、工具编号、同日顺序号）排序的
事件；每件工具的状态由调用方保管（内存字典或数据库表），引擎自身只持有
计数器、月度勾稽和上一笔事件，因此同一件工具的记录再多也能算完。

引擎状态可整体序列化为 JSON（dump/restore），盘点中断后从检查点续跑，
已提交的批次不会重复计数。无论分多少批、是否中途续跑，各月在管数和期末
工具状态都与整簿一次性回放完全一致。
"""

from calendar import monthrange
from collections import Counter
from dataclasses import dataclass
from datetime import date, timedelta

# 站里机器内存一次最多暂放的交接簿笔数；入库分片与回放分页都以此为上限。
BATCH_SIZE = 2000

TRANSITIONS = {
    (None, "accept"): "in_custody",
    ("in_custody", "check-out"): "checked_out",
    ("checked_out", "check-in"): "in_custody",
    ("in_custody", "retire"): "retired",
}
STATUS_LABELS = {None: "未接收", "in_custody": "在管", "checked_out": "借出", "retired": "退役"}
EVENT_LABELS = {"accept": "接收", "check-out": "借出", "check-in": "归还", "retire": "退役"}

BALANCE_COLUMNS = (
    ("start_date", "统计首日"), ("as_of_date", "截止日"), ("close_type", "快照类型"),
    ("opening_active", "期初有效"), ("opening_in_custody", "期初在管"),
    ("opening_checked_out", "期初借出"), ("accepted", "接收"), ("checked_out", "借出"),
    ("checked_in", "归还"), ("retired", "退役"), ("closing_active", "期末有效"),
    ("closing_in_custody", "期末在管"), ("closing_checked_out", "期末借出"),
    ("in_custody_rate_pct", "期末在管率（%）"), ("active_difference", "有效勾稽差额"),
    ("custody_difference", "在管勾稽差额"), ("checked_out_difference", "借出勾稽差额"),
)
ASSET_COLUMNS = (
    ("asset_id", "工具编号"), ("asset_name", "工具名称"), ("status", "期末状态"),
    ("last_event_date", "末笔交接日"), ("last_sequence", "末笔顺序号"),
    ("last_event_type", "末笔交接类型"), ("last_event_id", "末笔事件编号"),
)


@dataclass(frozen=True)
class Event:
    event_id: str
    asset_id: str
    asset_name: str | None
    event_date: date
    sequence: int
    event_type: str

    def pack(self):
        return [self.event_id, self.asset_id, self.asset_name,
                self.event_date.isoformat(), self.sequence, self.event_type]

    @classmethod
    def unpack(cls, raw):
        event_id, asset_id, asset_name, day, sequence, event_type = raw
        return cls(event_id, asset_id, asset_name, date.fromisoformat(day), sequence, event_type)


@dataclass
class AssetState:
    """单件工具的回放状态；由调用方按批存取，引擎不长期持有。"""

    name: str
    status: str
    last_event: Event


def _month_end(day):
    return day.replace(day=monthrange(day.year, day.month)[1])


def _balance_row(start, close, close_type, opening, closing, movements):
    opening_active, opening_custody, opening_out = opening
    active, custody, out = closing
    accepted = movements["accept"]
    checked_out = movements["check-out"]
    checked_in = movements["check-in"]
    retired = movements["retire"]
    return {
        "start_date": start.isoformat(), "as_of_date": close.isoformat(), "close_type": close_type,
        "opening_active": opening_active, "opening_in_custody": opening_custody,
        "opening_checked_out": opening_out,
        "accepted": accepted, "checked_out": checked_out, "checked_in": checked_in, "retired": retired,
        "closing_active": active, "closing_in_custody": custody, "closing_checked_out": out,
        "in_custody_rate_pct": round(100 * custody / active, 4) if active else None,
        "active_difference": active - (opening_active + accepted - retired),
        "custody_difference": custody - (opening_custody + accepted + checked_in - checked_out - retired),
        "checked_out_difference": out - (opening_out + checked_out - checked_in),
    }


class Replay:
    """按批回放已排序事件，产出逐月勾稽与整段期初期末。

    - consume() 每次接收不超过 BATCH_SIZE 条事件，states 为该批涉及工具的
      状态字典（{asset_id: AssetState | None}），就地更新后由调用方落盘。
    - 同号同内容的重抄记录只回放一次：相同内容在排序后必然相邻，与上一笔
      完全一致的事件直接跳过（上一笔随检查点保存，跨批也不会重复计数）。
    - 全部批次消费完后调用 close() 补齐剩余月份。
    """

    def __init__(self, start, cutoff):
        if start > cutoff:
            raise ValueError("统计首日须早于或等于盘点截止日。")
        self.start = start
        self.cutoff = cutoff
        self.counts = Counter()
        self.totals = Counter()
        self.monthly = []
        self._month_start = start
        self._boundary = min(_month_end(start), cutoff)
        self._opened = False
        self._opening = (0, 0, 0)
        self._month_opening = (0, 0, 0)
        self._month_movements = Counter()
        self._previous = None

    def balance(self):
        in_custody = self.counts["in_custody"]
        checked_out = self.counts["checked_out"]
        return in_custody + checked_out, in_custody, checked_out

    def _take_opening(self):
        self._opening = self.balance()
        self._month_opening = self._opening
        self._opened = True

    def _apply(self, event, states, count_movement):
        state = states.get(event.asset_id)
        prior = state.status if state else None
        status = TRANSITIONS.get((prior, event.event_type))
        if status is None:
            raise ValueError(f"事件 {event.event_id}（工具 {event.asset_id}，{event.event_date}）"
                             f"无法执行{EVENT_LABELS[event.event_type]}，当前状态为{STATUS_LABELS[prior]}。")
        if state:
            self.counts[prior] -= 1
        name = state.name if state else event.asset_name
        states[event.asset_id] = AssetState(name, status, event)
        self.counts[status] += 1
        if count_movement:
            self.totals[event.event_type] += 1
            self._month_movements[event.event_type] += 1

    def _close_month(self):
        close_type = "自然月末" if self._boundary == _month_end(self._month_start) else "截止日"
        self.monthly.append(_balance_row(self._month_start, self._boundary, close_type,
                                         self._month_opening, self.balance(), self._month_movements))
        self._month_start = self._boundary + timedelta(days=1)
        self._boundary = min(_month_end(self._month_start), self.cutoff)
        self._month_opening = self.balance()
        self._month_movements = Counter()

    def consume(self, events, states):
        """消费一批已排序事件；返回 False 表示后续事件已超出截止日，可停止取数。"""
        for event in events:
            if event.event_date > self.cutoff:
                return False
            if event == self._previous:
                continue
            self._previous = event
            if event.event_date < self.start:
                self._apply(event, states, count_movement=False)
                continue
            if not self._opened:
                self._take_opening()
            while event.event_date > self._boundary:
                self._close_month()
            self._apply(event, states, count_movement=True)
        return True

    def close(self):
        """全部事件消费完后补齐到截止日的各期快照。"""
        if not self._opened:
            self._take_opening()
        while True:
            boundary = self._boundary
            self._close_month()
            if boundary == self.cutoff:
                break

    def period_row(self):
        return _balance_row(self.start, self.cutoff, "区间期末", self._opening, self.balance(), self.totals)

    def dump(self):
        """序列化检查点；配合状态表可在中断后从原进度续跑。"""
        return {
            "counts": dict(self.counts), "totals": dict(self.totals), "monthly": self.monthly,
            "month_start": self._month_start.isoformat(), "boundary": self._boundary.isoformat(),
            "opened": self._opened, "opening": list(self._opening),
            "month_opening": list(self._month_opening), "month_movements": dict(self._month_movements),
            "previous": self._previous.pack() if self._previous else None,
        }

    @classmethod
    def restore(cls, start, cutoff, payload):
        replay = cls(start, cutoff)
        replay.counts = Counter(payload["counts"])
        replay.totals = Counter(payload["totals"])
        replay.monthly = list(payload["monthly"])
        replay._month_start = date.fromisoformat(payload["month_start"])
        replay._boundary = date.fromisoformat(payload["boundary"])
        replay._opened = payload["opened"]
        replay._opening = tuple(payload["opening"])
        replay._month_opening = tuple(payload["month_opening"])
        replay._month_movements = Counter(payload["month_movements"])
        replay._previous = Event.unpack(payload["previous"]) if payload["previous"] else None
        return replay


def _table(title, columns, rows):
    return {"title": title, "columns": [{"key": key, "label": label} for key, label in columns], "rows": rows}


def build_result(replay, asset_items):
    """汇总回放结果。asset_items 为按工具编号排序的 (asset_id, AssetState) 迭代，
    调用方可分页供给，引擎不要求全部资产同时驻留内存。"""
    start, cutoff = replay.start, replay.cutoff
    period = replay.period_row()
    monthly = replay.monthly
    active, custody, out = replay.balance()
    assets = [
        {"asset_id": asset_id, "asset_name": asset.name, "status": STATUS_LABELS[asset.status],
         "last_event_date": asset.last_event.event_date.isoformat(),
         "last_sequence": asset.last_event.sequence,
         "last_event_type": EVENT_LABELS[asset.last_event.event_type],
         "last_event_id": asset.last_event.event_id}
        for asset_id, asset in asset_items
    ]
    reconciled = all(row[key] == 0 for row in [period, *monthly]
                     for key in ("active_difference", "custody_difference", "checked_out_difference"))
    return {
        "state": "ok" if active and reconciled else "warning",
        "headline": f"截至 {cutoff}，有效工具 {active} 件，在管 {custody} 件，借出 {out} 件。",
        "note": f"统计期间为 {start} 至 {cutoff}，含首尾两日。期末取截止日日终，流量按事件发生日归属。",
        "metrics": [
            {"label": "期末有效资产", "value": active, "detail": "在管与借出工具之和"},
            {"label": "期末在管", "value": custody, "detail": "已接收或已归还且仍在站内的工具"},
            {"label": "期末借出", "value": out, "detail": "截至期末仍未归还"},
            {"label": "期末在管率（%）", "value": period["in_custody_rate_pct"],
             "detail": f"{custody} / {active}" if active else "有效资产为 0，在管率留空"},
        ],
        "tables": [
            _table("区间期初期末勾稽", BALANCE_COLUMNS, [period]),
            _table("月末与截止日勾稽", BALANCE_COLUMNS, monthly),
            _table("期末资产快照", ASSET_COLUMNS, assets),
        ],
        "charts": [
            {"title": title, "kind": "line",
             "series": [{"label": row["as_of_date"], "value": row[key]} for row in monthly]}
            for title, key in (("各盘点日有效资产（件）", "closing_active"), ("各盘点日在管工具（件）", "closing_in_custody"))
        ],
        "sections": [{"title": "勾稽与盘点复核", "text": (
            ("各期有效、在管及借出数量的勾稽差额均为 0。" if reconciled else "存在勾稽差额，请核对交接事件。")
            + "逐月期末存量与比率按截止日取值。复核时可从期末资产快照定位末笔交接单。"
        )}],
    }
