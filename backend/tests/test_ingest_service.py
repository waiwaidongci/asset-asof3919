"""入库服务层单测：以 sqlalchemy 替身驱动真实 ingest 代码。

用法：python3 -I tests/test_ingest_service.py

覆盖：分片入库进度、重复分片不重复计数、乱序分片报错、收尾校验
（同号异件指出原行、同槽重复）、回放序号编排、同簿去重。
"""
import sqlite3
import sys
import types
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))


# ---- sqlalchemy 最小替身 -------------------------------------------------
class IntegrityError(Exception):
    pass


class Col:
    def __init__(self, name):
        self.name = name

    def __eq__(self, other):
        return ('eq', self.name, other)

    def __ne__(self, other):
        return ('ne', self.name, other)


class Select:
    def __init__(self, entity, delete=False):
        self.entity = entity
        self.conds = []
        self.delete = delete

    def where(self, *conds):
        self.conds.extend(conds)
        return self


sa = types.ModuleType('sqlalchemy')
sa.select = lambda entity: Select(entity)
sa.delete = lambda entity: Select(entity, delete=True)
sa.text = lambda sql: ('text', sql)
sa_exc = types.ModuleType('sqlalchemy.exc')
sa_exc.IntegrityError = IntegrityError
sa.exc = sa_exc
sys.modules['sqlalchemy'] = sa
sys.modules['sqlalchemy.exc'] = sa_exc


# ---- 模型替身 -------------------------------------------------------------
class Dataset:
    _next_id = [1]
    name = Col('name')
    checksum = Col('checksum')
    id = Col('id')
    status = Col('status')

    def __init__(self, **kwargs):
        self.id = None
        self.event_count = None
        self.error = None
        self.checksum = None
        for key, value in kwargs.items():
            setattr(self, key, value)


class LedgerChunk:
    dataset_id = Col('dataset_id')
    chunk_index = Col('chunk_index')

    def __init__(self, **kwargs):
        for key, value in kwargs.items():
            setattr(self, key, value)


class LedgerEvent:
    dataset_id = Col('dataset_id')

    def __init__(self, **kwargs):
        for key, value in kwargs.items():
            setattr(self, key, value)


models = types.ModuleType('src.models.source')
models.Dataset = Dataset
models.LedgerChunk = LedgerChunk
models.LedgerEvent = LedgerEvent
sys.modules['src.models.source'] = models

from src.services import ingest  # noqa: E402
from src.services.ingest import append_chunk, begin, finalize, ingest_rows  # noqa: E402


class FakeDB:
    """以 sqlite 执行真实 SQL，以内存表模拟 ORM 存取。"""

    def __init__(self):
        self.datasets = {}
        self.chunks = {}
        self.events = []
        self.sql = sqlite3.connect(':memory:')
        self.sql.execute(
            'CREATE TABLE ledger_events (id INTEGER PRIMARY KEY, dataset_id INTEGER,'
            ' chunk_index INTEGER, source_row INTEGER, event_id TEXT, asset_id TEXT,'
            ' asset_name TEXT, event_date TEXT, sequence INTEGER, event_type TEXT,'
            ' content_hash TEXT, replay_order INTEGER)')

    # -- ORM 面 --
    def add(self, obj):
        if isinstance(obj, Dataset):
            if obj.id is None:
                obj.id = Dataset._next_id[0]
                Dataset._next_id[0] += 1
            self.datasets[obj.id] = obj
        elif isinstance(obj, LedgerChunk):
            key = (obj.dataset_id, obj.chunk_index)
            if key in self.chunks:
                raise IntegrityError('dup chunk')
            self.chunks[key] = obj
        elif isinstance(obj, LedgerEvent):
            obj.id = len(self.events) + 1
            self.events.append(obj)
            self.sql.execute(
                'INSERT INTO ledger_events (id, dataset_id, chunk_index, source_row, event_id,'
                ' asset_id, asset_name, event_date, sequence, event_type, content_hash)'
                ' VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
                (obj.id, obj.dataset_id, obj.chunk_index, obj.source_row, obj.event_id,
                 obj.asset_id, obj.asset_name, obj.event_date, obj.sequence, obj.event_type,
                 obj.content_hash))
        else:
            raise AssertionError(obj)

    def flush(self):
        seen = {}
        for obj in self.datasets.values():
            if obj.checksum is None:
                continue
            key = (obj.name, obj.checksum)
            if key in seen and seen[key] is not obj:
                raise IntegrityError('unique name+checksum')
            seen[key] = obj

    def commit(self):
        pass

    def delete(self, obj):
        self.datasets.pop(obj.id, None)

    def begin_nested(self):
        class Nest:
            def __enter__(self):
                return self

            def __exit__(self, *exc):
                return False
        return Nest()

    def _match(self, obj, conds):
        for op, name, value in conds:
            actual = getattr(obj, name)
            if op == 'eq' and actual != value:
                return False
            if op == 'ne' and actual == value:
                return False
        return True

    def scalar(self, sel):
        store = {Dataset: self.datasets, LedgerChunk: self.chunks}[sel.entity]
        for obj in store.values():
            if self._match(obj, sel.conds):
                return obj
        return None

    def execute(self, stmt, params=None):
        if isinstance(stmt, Select) and stmt.delete:
            entity = stmt.entity
            assert entity is LedgerEvent or entity is LedgerChunk
            store = self.events if entity is LedgerEvent else self.chunks
            if entity is LedgerEvent:
                self.events[:] = [e for e in self.events if not self._match(e, stmt.conds)]
                self.sql.execute('DELETE FROM ledger_events WHERE dataset_id = ?',
                                 (stmt.conds[0][2],))
            else:
                for key in [k for k, c in self.chunks.items() if self._match(c, stmt.conds)]:
                    del self.chunks[key]
            return None
        kind, sqltext = stmt
        assert kind == 'text'
        cursor = self.sql.execute(sqltext, params or {})

        class Result:
            def first(self):
                return cursor.fetchone()

            def all(self):
                return cursor.fetchall()

            def scalar_one(self):
                return cursor.fetchone()[0]
        return Result()


def ledger_rows(count, tools=3, dup_last=False):
    rows = []
    serial = 0
    for tool in range(tools):
        asset = f'GQ-{tool:03d}'
        serial += 1
        rows.append({'event_id': f'E{serial:05d}', 'asset_id': asset, 'asset_name': f'工具{tool}',
                     'event_date': '2026-08-15', 'sequence': 1, 'event_type': 'accept'})
        per = count // tools
        for seq in range(2, per + 1):
            serial += 1
            kind = 'check-out' if seq % 2 == 0 else 'check-in'
            day = f'2026-{9 + (seq // 3000):02d}-{(seq % 28) + 1:02d}'
            rows.append({'event_id': f'E{serial:05d}', 'asset_id': asset, 'asset_name': None,
                         'event_date': day, 'sequence': seq, 'event_type': kind})
    if dup_last:
        rows.append(dict(rows[-1]))
    return rows


def expect_error(label, fn, fragment):
    try:
        fn()
    except ValueError as error:
        assert fragment in str(error), f'{label}: 「{fragment}」不在「{error}」中'
        print(f'  ok {label}: {error}')
        return str(error)
    raise AssertionError(f'{label} 应报错')


def main():
    db = FakeDB()
    rows = ledger_rows(4500)
    chunks = [rows[i:i + 2000] for i in range(0, len(rows), 2000)]
    assert len(chunks) == 3

    dataset = begin(db, '往年交接簿', '桂桥工具流转_2023-2026.csv', 'analyst')
    state = append_chunk(db, dataset, 0, chunks[0])
    assert state['imported_rows'] == 2000 and state['next_chunk'] == 1
    print('  ok 第 1 批入库，进度 2000 行')

    again = append_chunk(db, dataset, 0, chunks[0])
    assert again['imported_rows'] == 2000 and again['next_chunk'] == 1
    assert len(db.events) == 2000, '重复分片不应重复计数'
    print('  ok 重复提交第 1 批不重复计数')

    expect_error('分片内容不一致', lambda: append_chunk(db, dataset, 0, chunks[1]), '不一致')
    expect_error('分片跳号', lambda: append_chunk(db, dataset, 2, chunks[2]), '第 2 批')

    append_chunk(db, dataset, 1, chunks[1])
    state = append_chunk(db, dataset, 2, chunks[2])
    assert state['imported_rows'] == 4500 and state['next_chunk'] == 3
    print('  ok 中断后按 next_chunk 续传入库')

    item, created = finalize(db, dataset)
    assert created and item.status == 'ready' and item.row_count == 4500
    assert item.event_count == 4500 and item.checksum
    ordered = db.sql.execute('SELECT event_date, asset_id, sequence FROM ledger_events'
                             ' ORDER BY replay_order').fetchall()
    assert ordered == sorted(ordered), '回放序号须按日期、工具、顺序号编排'
    print('  ok 收尾后批次就绪，回放序号按（日期、工具、顺序号）编排')

    item2, created2 = finalize(db, dataset)
    assert not created2 and item2 is item
    print('  ok 重复收尾幂等')

    db2 = FakeDB()
    db2.datasets[item.id] = item  # 同名同内容的就绪批次已存在
    same, created_same = ingest_rows(db2, '往年交接簿', '重传.csv', rows, 'analyst')
    assert not created_same and same.id == item.id
    assert not any(e.dataset_id != item.id for e in db2.events), '重复批次的事件应被清理'
    print('  ok 同名同内容只留一个就绪批次')

    bad = ledger_rows(20)
    bad.append({**bad[5], 'asset_name': '被改动的名称'})  # 同号异件
    db3 = FakeDB()
    conflict = begin(db3, '冲突簿', 'bad.csv', 'analyst')
    append_chunk(db3, conflict, 0, bad)
    message = expect_error('同号异件指出原行', lambda: finalize(db3, conflict), '第 7 行与第 20 行')
    assert bad[5]['event_id'] in message

    bad_slot = ledger_rows(20)
    bad_slot.append({**bad_slot[6], 'event_id': 'E99999'})  # 同槽不同号
    db4 = FakeDB()
    slot = begin(db4, '同槽簿', 'slot.csv', 'analyst')
    append_chunk(db4, slot, 0, bad_slot)
    expect_error('同槽重复', lambda: finalize(db4, slot), '顺序号')

    expect_error('接收缺名称', lambda: ingest_rows(FakeDB(), '缺名称', 'x.csv',
                 [{'event_id': 'E1', 'asset_id': 'GQ-1', 'asset_name': None,
                   'event_date': '2026-09-01', 'sequence': 1, 'event_type': 'accept'}], 'a'),
                 '工具名称')
    expect_error('分片超两千行', lambda: append_chunk(FakeDB(), begin(FakeDB(), 'x', 'x.csv', 'a'),
                                                      0, ledger_rows(2001)), '2000')
    expect_error('未知列', lambda: ingest_rows(FakeDB(), '多列', 'x.csv',
                 [{**ledger_rows(1)[0], 'extra': 'x'}], 'a'), '列名')
    print('入库服务层测试全部通过。')


if __name__ == '__main__':
    main()
