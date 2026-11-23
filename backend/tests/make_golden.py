"""生成回放引擎的金样数据：用重构前的 analyze() 输出作为基准。

用法：python3 -I tests/make_golden.py
金样写入 tests/golden/，供 test_replay.py 比对。
"""
import json
import random
import sys
from datetime import date, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.domain.analysis import analyze  # noqa: E402
from src.domain.demo import demo_rows  # noqa: E402

GOLDEN = Path(__file__).resolve().parent / 'golden'


def make_ledger(seed, n_tools, n_events, first, last, dup_rate=0.08, retire_rate=0.005):
    """生成合法生命周期的事件流，注入重复行并乱序。"""
    rng = random.Random(seed)
    days = (date.fromisoformat(last) - date.fromisoformat(first)).days
    rows = []
    serial = 0
    for tool in range(n_tools):
        asset = f'GQ-{tool + 1:03d}'
        name = f'工具{tool + 1:03d}'
        status = None
        quota = max(1, n_events // n_tools)
        sequence = 0
        for k in range(quota):
            # 日期随事件序号单调推进，保证生命周期在回放顺序下合法
            day = date.fromisoformat(first) + timedelta(days=(k * days) // max(quota - 1, 1))
            if status is None:
                kind = 'accept'
            elif status == 'in_custody':
                kind = 'retire' if rng.random() < retire_rate else 'check-out'
            else:
                kind = 'check-in'
            sequence += 1
            serial += 1
            rows.append({'event_id': f'E{serial:06d}', 'asset_id': asset,
                         'asset_name': name if kind == 'accept' else None,
                         'event_date': day.isoformat(), 'sequence': sequence, 'event_type': kind})
            status = {'accept': 'in_custody', 'check-out': 'checked_out',
                      'check-in': 'in_custody', 'retire': 'retired'}[kind]
            if status == 'retired':
                break
    shuffled = rows[:]
    rng.shuffle(shuffled)
    for row in shuffled:
        if rng.random() < dup_rate:
            rows.append(dict(row))  # 重抄的同号同内容
    rng.shuffle(rows)
    return rows


CASES = []


def add_case(name, rows, params):
    CASES.append((name, rows, params))


def main():
    GOLDEN.mkdir(exist_ok=True)
    demo = demo_rows()
    add_case('demo-period', demo, {'start_date': '2026-09-01', 'as_of_date': '2026-11-20'})
    add_case('demo-next-day', demo, {'start_date': '2026-09-01', 'as_of_date': '2026-11-21'})
    add_case('demo-fixture', demo, {'start_date': '2026-09-01', 'as_of_date': '2026-10-31'})
    add_case('demo-mid-month', demo, {'start_date': '2026-09-15', 'as_of_date': '2026-10-20'})
    add_case('demo-single-day', demo, {'start_date': '2026-10-31', 'as_of_date': '2026-10-31'})
    add_case('demo-before-all', demo, {'start_date': '2027-01-01', 'as_of_date': '2027-02-15'})

    many_tools = make_ledger(2611, 40, 6000, '2026-08-01', '2026-11-30')
    add_case('many-tools', many_tools, {'start_date': '2026-09-01', 'as_of_date': '2026-11-20'})

    one_tool = make_ledger(7, 1, 4500, '2026-08-01', '2026-11-30', retire_rate=0)
    add_case('one-tool-over-batch', one_tool, {'start_date': '2026-09-01', 'as_of_date': '2026-11-20'})

    long_span = make_ledger(99, 25, 3000, '2023-01-05', '2026-11-30')
    add_case('long-span', long_span, {'start_date': '2024-06-11', 'as_of_date': '2026-11-20'})

    manifest = []
    for name, rows, params in CASES:
        result = analyze(rows, params)
        payload = {'rows': rows, 'params': params, 'result': result}
        (GOLDEN / f'{name}.json').write_text(json.dumps(payload, ensure_ascii=False), encoding='utf-8')
        manifest.append({'name': name, 'rows': len(rows), 'params': params,
                         'headline': result['headline']})
    (GOLDEN / 'manifest.json').write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding='utf-8')
    print(json.dumps(manifest, ensure_ascii=False, indent=2))


if __name__ == '__main__':
    main()
