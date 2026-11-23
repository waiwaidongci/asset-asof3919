"""入库收尾 SQL 与后台分批回放的数据库级回归（SQLite 镜像验证）。

用法：python3 -I tests/test_ingest_sql.py

- ledger_sql 中的集合操作在 sqlite3 上执行，验证同号异件、同槽重复、
  回放序号编排与去重计数的逻辑（线上由 SQLAlchemy 在 PostgreSQL 执行同一 SQL）。
- 按 execution.py 的算法在 sqlite3 上模拟：每批 2000 笔提交一次断点，
  中途“崩溃”后从断点续跑，结果须与整簿 analyze() 金样一致。
"""
import json
import sqlite3
import sys
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from make_golden import make_ledger  # noqa: E402
from src.domain.analysis import analyze  # noqa: E402
from src.domain.replay import BATCH_SIZE, AssetState, Event, Replay, build_result  # noqa: E402
from src.services import ledger_sql as sql  # noqa: E402

SCHEMA = """
CREATE TABLE ledger_events (
    id INTEGER PRIMARY KEY,
    dataset_id INTEGER NOT NULL,
    chunk_index INTEGER NOT NULL,
    source_row INTEGER NOT NULL,
    event_id TEXT NOT NULL,
    asset_id TEXT NOT NULL,
    asset_name TEXT,
    event_date TEXT NOT NULL,
    sequence INTEGER NOT NULL,
    event_type TEXT NOT NULL,
    content_hash TEXT NOT NULL,
    replay_order INTEGER
);
CREATE TABLE run_checkpoints (
    run_id INTEGER PRIMARY KEY,
    cursor INTEGER NOT NULL,
    processed INTEGER NOT NULL,
    payload TEXT NOT NULL
);
CREATE TABLE run_asset_states (
    run_id INTEGER NOT NULL,
    asset_id TEXT NOT NULL,
    asset_name TEXT,
    status TEXT NOT NULL,
    last_event_date TEXT NOT NULL,
    last_sequence INTEGER NOT NULL,
    last_event_type TEXT NOT NULL,
    last_event_id TEXT NOT NULL,
    PRIMARY KEY (run_id, asset_id)
);
"""


def content_hash(row):
    return __import__('hashlib').sha256(
        json.dumps(row, ensure_ascii=False, sort_keys=True, separators=(',', ':')).encode()).hexdigest()


def insert_rows(db, rows, dataset=1):
    db.executemany(
        'INSERT INTO ledger_events (dataset_id, chunk_index, source_row, event_id, asset_id,'
        ' asset_name, event_date, sequence, event_type, content_hash)'
        ' VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
        [(dataset, i // BATCH_SIZE, i + 2, row['event_id'], row['asset_id'], row['asset_name'],
          row['event_date'], row['sequence'], row['event_type'], content_hash(row))
         for i, row in enumerate(rows)])


def finalize(db, dataset=1):
    conflict = db.execute(sql.SQL_CONFLICT, {'dataset': dataset}).fetchone()
    assert conflict is None, f'不应存在同号异件: {conflict}'
    slot = db.execute(sql.SQL_SLOT, {'dataset': dataset}).fetchone()
    assert slot is None, f'不应存在同槽重复: {slot}'
    db.execute(sql.SQL_REPLAY_ORDER, {'dataset': dataset})
    return db.execute(sql.SQL_EVENT_COUNT, {'dataset': dataset}).fetchone()[0]


def to_event(row):
    return Event(row[0], row[1], row[2], date.fromisoformat(row[3]), row[4], row[5])


def run_worker(db, params, crash_after=None, dataset=1, run_id=9):
    """镜像 execution.execute 的批处理循环；crash_after=N 表示第 N 批提交前崩溃。"""
    start = date.fromisoformat(params['start_date'])
    cutoff = date.fromisoformat(params['as_of_date'])
    batches = 0
    while True:
        row = db.execute('SELECT cursor, processed, payload FROM run_checkpoints WHERE run_id = ?',
                         (run_id,)).fetchone()
        replay = Replay(start, cutoff) if row is None else Replay.restore(start, cutoff, json.loads(row[2]))
        cursor = row[0] if row else 0
        page = db.execute(
            'SELECT event_id, asset_id, asset_name, event_date, sequence, event_type, replay_order'
            ' FROM ledger_events WHERE dataset_id = ? AND replay_order > ? AND event_date <= ?'
            ' ORDER BY replay_order LIMIT ?', (dataset, cursor, cutoff.isoformat(), BATCH_SIZE)).fetchall()
        if not page:
            states = []
            after = ''
            while True:
                chunk = db.execute(
                    'SELECT asset_id, asset_name, status, last_event_date, last_sequence,'
                    ' last_event_type, last_event_id FROM run_asset_states'
                    ' WHERE run_id = ? AND asset_id > ? ORDER BY asset_id LIMIT ?',
                    (run_id, after, BATCH_SIZE)).fetchall()
                if not chunk:
                    break
                for asset_id, name, status, day, seq, kind, event_id in chunk:
                    last = Event(event_id, asset_id, name, date.fromisoformat(day), seq, kind)
                    states.append((asset_id, AssetState(name, status, last)))
                after = chunk[-1][0]
            replay.close()
            db.execute('DELETE FROM run_checkpoints WHERE run_id = ?', (run_id,))
            db.execute('DELETE FROM run_asset_states WHERE run_id = ?', (run_id,))
            db.commit()
            return build_result(replay, states)
        asset_ids = {event[1] for event in page}
        marks = ','.join('?' * len(asset_ids))
        stored = db.execute(
            f'SELECT asset_id, asset_name, status, last_event_date, last_sequence,'
            f' last_event_type, last_event_id FROM run_asset_states'
            f' WHERE run_id = ? AND asset_id IN ({marks})', (run_id, *asset_ids)).fetchall()
        states = {}
        for asset_id, name, status, day, seq, kind, event_id in stored:
            last = Event(event_id, asset_id, name, date.fromisoformat(day), seq, kind)
            states[asset_id] = AssetState(name, status, last)
        replay.consume([to_event(event) for event in page], states)
        for asset_id in asset_ids:
            state = states.get(asset_id)
            if state is None:
                continue
            db.execute(
                'INSERT INTO run_asset_states (run_id, asset_id, asset_name, status, last_event_date,'
                ' last_sequence, last_event_type, last_event_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?)'
                ' ON CONFLICT (run_id, asset_id) DO UPDATE SET asset_name = excluded.asset_name,'
                ' status = excluded.status, last_event_date = excluded.last_event_date,'
                ' last_sequence = excluded.last_sequence, last_event_type = excluded.last_event_type,'
                ' last_event_id = excluded.last_event_id',
                (run_id, asset_id, state.name, state.status, state.last_event.event_date.isoformat(),
                 state.last_event.sequence, state.last_event.event_type, state.last_event.event_id))
        payload = json.dumps(replay.dump(), ensure_ascii=False)
        db.execute(
            'INSERT INTO run_checkpoints (run_id, cursor, processed, payload) VALUES (?, ?, ?, ?)'
            ' ON CONFLICT (run_id) DO UPDATE SET cursor = excluded.cursor,'
            ' processed = run_checkpoints.processed + excluded.processed, payload = excluded.payload',
            (run_id, page[-1][6], len(page), payload))
        batches += 1
        if crash_after is not None and batches >= crash_after:
            db.rollback()  # 本批未提交，模拟进程崩溃
            return None
        db.commit()


def run_with_crashes(db, params, crashes):
    for attempt, crash_at in enumerate(crashes):
        result = run_worker(db, params, crash_after=crash_at)
        if result is not None:
            return result
        print(f'  第 {attempt + 1} 次尝试在第 {crash_at} 批后中断，从断点续跑')
    return run_worker(db, params)


def check_conflict_detection():
    db = sqlite3.connect(':memory:')
    db.executescript(SCHEMA.split('CREATE TABLE run_checkpoints')[0])
    base = {'asset_name': None, 'sequence': 1, 'event_type': 'check-out'}
    rows = [
        {'event_id': 'E1', 'asset_id': 'GQ-1', 'asset_name': '电钻',
         'event_date': '2026-09-01', 'sequence': 1, 'event_type': 'accept'},
        {**base, 'event_id': 'E2', 'asset_id': 'GQ-1', 'event_date': '2026-09-02'},
        {**base, 'event_id': 'E2', 'asset_id': 'GQ-1', 'event_date': '2026-09-02'},  # 重抄同内容
        {**base, 'event_id': 'E9', 'asset_id': 'GQ-2', 'event_date': '2026-09-03'},
        {**base, 'event_id': 'E9', 'asset_id': 'GQ-2', 'event_date': '2026-09-04'},  # 同号异件
    ]
    insert_rows(db, rows)
    conflict = db.execute(sql.SQL_CONFLICT.replace(':dataset', '?'), (1,)).fetchone()
    assert conflict and conflict[0] == 'E9', conflict
    first = db.execute(sql.SQL_EVENT_FIRST, {'dataset': 1, 'event': 'E9'}).fetchone()
    other = db.execute(sql.SQL_EVENT_CONFLICT, {'dataset': 1, 'event': 'E9', 'hash': first[1]}).fetchone()
    assert first[0] == 5 and other[0] == 6, (first, other)  # 指出原行
    print('  ok 同号异件指出原行（第 5 行与第 6 行）')
    # 去掉异件，保留同槽不同号
    db.execute("DELETE FROM ledger_events WHERE event_id = 'E9' AND event_date = '2026-09-04'")
    db.execute("INSERT INTO ledger_events (dataset_id, chunk_index, source_row, event_id, asset_id,"
               " asset_name, event_date, sequence, event_type, content_hash)"
               " VALUES (1, 0, 7, 'E7', 'GQ-1', NULL, '2026-09-02', 1, 'check-out', 'x')")
    slot = db.execute(sql.SQL_SLOT.replace(':dataset', '?'), (1,)).fetchone()
    assert slot and slot[0] == 'GQ-1' and slot[1] == '2026-09-02' and slot[2] == 1, slot
    involved = db.execute(sql.SQL_SLOT_ROWS,
                          {'dataset': 1, 'asset': 'GQ-1', 'day': '2026-09-02', 'seq': 1}).fetchall()
    assert [row[1] for row in involved] == ['E2', 'E7'], involved
    print('  ok 同槽重复报出两个事件编号与原行')
    db.execute("DELETE FROM ledger_events WHERE event_id = 'E7'")
    assert db.execute(sql.SQL_SLOT, {'dataset': 1}).fetchone() is None
    count = finalize(db)
    assert count == 3, count  # E1、E2（重抄只算一次）、E9
    ordered = db.execute('SELECT event_id, replay_order FROM ledger_events ORDER BY replay_order').fetchall()
    assert [row[1] for row in ordered] == [1, 2, 3, 4], ordered
    assert ordered[1][0] == ordered[2][0] == 'E2', ordered  # 重抄相邻
    print('  ok 回放序号编排与去重计数（重抄相邻、同号同内容只算一次）')


def check_worker_resume():
    params = {'start_date': '2026-09-01', 'as_of_date': '2026-11-20'}
    rows = make_ledger(4242, 40, 6000, '2026-08-01', '2026-11-30')
    expected = analyze(rows, params)
    db = sqlite3.connect(':memory:')
    db.executescript(SCHEMA)
    insert_rows(db, rows)
    count = finalize(db)
    db.commit()
    print(f'  入库 {len(rows)} 行，去重后 {count} 笔，分 {(count + BATCH_SIZE - 1) // BATCH_SIZE} 批回放')
    once = run_worker(db, params)
    assert once == expected, '一次性回放与金样不一致'
    print('  ok 无中断分批回放与整簿金样一致')
    again = run_with_crashes(db, params, crashes=[1, 2, 3])
    assert again == expected, '多次中断续跑与金样不一致'
    print('  ok 每批后都中断的续跑结果与整簿金样一致')
    leftover = db.execute('SELECT COUNT(*) FROM run_checkpoints').fetchone()[0]
    leftover += db.execute('SELECT COUNT(*) FROM run_asset_states').fetchone()[0]
    assert leftover == 0, '完成后断点与状态应清理'


def main():
    print('入库收尾 SQL 检查')
    check_conflict_detection()
    print('后台分批回放断点续跑')
    check_worker_resume()
    print('全部入库与回放测试通过。')


if __name__ == '__main__':
    main()
