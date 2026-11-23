"""回放引擎回归测试。

用法：python3 -I tests/test_replay.py

1. 重构后的 analyze() 输出必须与金样（重构前实现）逐字节一致。
2. 引擎按 BATCH_SIZE 分批、每批后序列化检查点并重建续跑，结果不变。
3. 同号同内容的重抄记录跨批也只计一次；单件工具记录超过 BATCH_SIZE 也能算完。
"""
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.domain.analysis import analyze  # noqa: E402
from src.domain.replay import BATCH_SIZE, Event, Replay, build_result  # noqa: E402

GOLDEN = Path(__file__).resolve().parent / 'golden'


def to_events(rows):
    return [Event(row['event_id'], row['asset_id'], row['asset_name'],
                  __import__('datetime').date.fromisoformat(row['event_date']),
                  row['sequence'], row['event_type']) for row in rows]


def sorted_events(rows):
    return sorted(to_events(rows),
                  key=lambda e: (e.event_date, e.asset_id, e.sequence))


def replay_in_batches(events, params, batch_size, crash_every=None):
    """分批回放；crash_every=N 表示每批后都模拟中断并从检查点重建。"""
    from datetime import date
    start = date.fromisoformat(params['start_date'])
    cutoff = date.fromisoformat(params['as_of_date'])
    replay = Replay(start, cutoff)
    states = {}
    persisted_states = dict(states)
    cursor = 0
    ordered = sorted_events(events)
    while cursor < len(ordered):
        batch = ordered[cursor:cursor + batch_size]
        replay.consume(batch, states)
        cursor += len(batch)
        payload = replay.dump()
        persisted_states = dict(states)
        if crash_every:
            # 模拟中断：丢弃内存，只从检查点与状态表恢复
            replay = Replay.restore(start, cutoff, json.loads(json.dumps(payload)))
            states = dict(persisted_states)
    replay.close()
    return build_result(replay, sorted(states.items()))


def check_equal(label, expected, actual):
    if expected != actual:
        diff = json.dumps({'expected': expected, 'actual': actual}, ensure_ascii=False, indent=1)
        raise AssertionError(f'{label} 不一致：\n{diff[:3000]}')
    print(f'  ok {label}')


def main():
    manifest = json.loads((GOLDEN / 'manifest.json').read_text(encoding='utf-8'))
    for entry in manifest:
        name = entry['name']
        payload = json.loads((GOLDEN / f'{name}.json').read_text(encoding='utf-8'))
        rows, params, expected = payload['rows'], payload['params'], payload['result']
        print(f'案例 {name}（{len(rows)} 行）')
        check_equal('analyze() 与金样一致', expected, analyze(rows, params))
        events = sorted_events(rows)
        check_equal('整批回放与金样一致', expected,
                    replay_in_batches(rows, params, batch_size=max(len(rows), 1)))
        check_equal('BATCH_SIZE 分批与金样一致', expected,
                    replay_in_batches(rows, params, batch_size=BATCH_SIZE))
        check_equal('小批（7 笔）与金样一致', expected,
                    replay_in_batches(rows, params, batch_size=7))
        check_equal('每批后中断续跑与金样一致', expected,
                    replay_in_batches(rows, params, batch_size=BATCH_SIZE, crash_every=1))
        check_equal('小批中断续跑与金样一致', expected,
                    replay_in_batches(rows, params, batch_size=13, crash_every=1))

    # 同号同内容恰好跨批：第 2000 与 2001 笔相同，只能计一次
    from datetime import date, timedelta
    rows = []
    serial = 0
    day = date(2026, 9, 1)
    for tool in range(3):
        asset = f'GQ-{tool:03d}'
        serial += 1
        rows.append({'event_id': f'E{serial:05d}', 'asset_id': asset, 'asset_name': f'工具{tool}',
                     'event_date': '2026-08-15', 'sequence': 1, 'event_type': 'accept'})
        for seq in range(2, 2100):
            serial += 1
            kind = 'check-out' if seq % 2 == 0 else 'check-in'
            day_of = (day + timedelta(days=seq // 40)).isoformat()
            rows.append({'event_id': f'E{serial:05d}', 'asset_id': asset, 'asset_name': None,
                         'event_date': day_of, 'sequence': seq, 'event_type': kind})
    # 复制第 2000 笔（排序后）制造跨批重复
    ordered = sorted_events(rows)
    dup = ordered[1999]
    rows.append({'event_id': dup.event_id, 'asset_id': dup.asset_id, 'asset_name': dup.asset_name,
                 'event_date': dup.event_date.isoformat(), 'sequence': dup.sequence,
                 'event_type': dup.event_type})
    params = {'start_date': '2026-09-01', 'as_of_date': '2026-11-30'}
    expected = replay_in_batches(rows, params, batch_size=10_000)
    check_equal('跨批重复只计一次', expected, replay_in_batches(rows, params, batch_size=BATCH_SIZE))
    check_equal('跨批重复中断续跑', expected,
                replay_in_batches(rows, params, batch_size=BATCH_SIZE, crash_every=1))
    print('全部回放引擎测试通过。')


if __name__ == '__main__':
    main()
