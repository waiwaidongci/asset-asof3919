"""大簿分批入库。

交接簿按每批最多 BATCH_SIZE 笔分片入库，每片提交即落盘并记录指纹；
中断后按 next_chunk 从原进度续传，重复分片按指纹跳过、不重复计数。
收尾（finalize）在数据库内完成同号异件、同槽重复检查并编排回放序号，
批次进入 ready 后才供查阅与盘点。
"""
import hashlib
import json
from datetime import date
from sqlalchemy import delete, select, text
from sqlalchemy.exc import IntegrityError
from src.domain.replay import BATCH_SIZE
from src.domain.spec import SPEC
from src.models.source import Dataset, LedgerChunk, LedgerEvent
from src.services import ledger_sql as sql
from src.services.validation import cell

_FIELDS = SPEC['fields']
_KEYS = {field['key'] for field in _FIELDS}


def _hash(text_value):
    return hashlib.sha256(text_value.encode()).hexdigest()


def _event_hash(parsed):
    raw = json.dumps(parsed, ensure_ascii=False, sort_keys=True, separators=(',', ':'))
    return _hash(raw)


def _parse_chunk(rows, first_row):
    """校验并规范化一批原始行，返回 (解析行, 原簿行号) 列表。"""
    if not isinstance(rows, list) or not rows:
        raise ValueError('分片至少需要一行数据')
    if len(rows) > BATCH_SIZE:
        raise ValueError(f'每个分片最多 {BATCH_SIZE} 行，请调整分批大小')
    parsed_rows = []
    for position, raw in enumerate(rows):
        source_row = first_row + position
        if not isinstance(raw, dict) or set(raw) - _KEYS:
            raise ValueError(f'CSV 第 {source_row} 行 · 列名与字段说明不符，请下载模板检查列名')
        try:
            parsed = {field['key']: cell(field, raw.get(field['key'])) for field in _FIELDS}
            if parsed['event_type'] == 'accept' and not parsed['asset_name']:
                raise ValueError('工具名称（接收时必填）不能为空')
        except ValueError as error:
            raise ValueError(f'CSV 第 {source_row} 行 · {error}') from None
        parsed_rows.append((parsed, source_row))
    return parsed_rows


def begin(db, name, filename, actor):
    dataset = Dataset(name=name, filename=filename, author=actor, status='importing',
                      imported_rows=0, next_chunk=0, row_count=0, rolling_hash=_hash(''))
    db.add(dataset)
    db.flush()
    return dataset


def progress(dataset):
    return {'id': dataset.id, 'name': dataset.name, 'filename': dataset.filename,
            'status': dataset.status, 'imported_rows': dataset.imported_rows,
            'next_chunk': dataset.next_chunk, 'event_count': dataset.event_count,
            'error': dataset.error}


def append_chunk(db, dataset, index, rows):
    """追加一个分片；调用方需已用行锁锁定 dataset。重复分片不重复计数。"""
    if dataset.status != 'importing':
        raise ValueError('该批次已结束入库，不能继续追加分片')
    parsed_rows = _parse_chunk(rows, dataset.imported_rows + 2)  # 原簿第 1 行为表头
    chunk_checksum = _hash(json.dumps([_event_hash(parsed) for parsed, _ in parsed_rows],
                                      separators=(',', ':')))
    if index < dataset.next_chunk:
        stored = db.scalar(select(LedgerChunk).where(LedgerChunk.dataset_id == dataset.id,
                                                     LedgerChunk.chunk_index == index))
        if stored and stored.row_count == len(parsed_rows) and stored.checksum == chunk_checksum:
            return progress(dataset)
        raise ValueError(f'第 {index + 1} 批与已入库内容不一致，请核对原簿后重新导入')
    if index > dataset.next_chunk:
        raise ValueError(f'分片顺序有误，下一个应上传第 {dataset.next_chunk + 1} 批')
    db.add(LedgerChunk(dataset_id=dataset.id, chunk_index=index,
                       row_count=len(parsed_rows), checksum=chunk_checksum))
    rolling = dataset.rolling_hash
    for parsed, source_row in parsed_rows:
        content_hash = _event_hash(parsed)
        rolling = _hash(rolling + content_hash)
        db.add(LedgerEvent(dataset_id=dataset.id, chunk_index=index, source_row=source_row,
                           event_id=parsed['event_id'], asset_id=parsed['asset_id'],
                           asset_name=parsed['asset_name'],
                           event_date=date.fromisoformat(parsed['event_date']),
                           sequence=parsed['sequence'], event_type=parsed['event_type'],
                           content_hash=content_hash))
    dataset.rolling_hash = rolling
    dataset.imported_rows += len(parsed_rows)
    dataset.next_chunk += 1
    db.flush()
    return progress(dataset)


def _check_conflicts(db, dataset_id):
    conflict = db.execute(text(sql.SQL_CONFLICT), {'dataset': dataset_id}).first()
    if conflict:
        event_id = conflict[0]
        first = db.execute(text(sql.SQL_EVENT_FIRST), {'dataset': dataset_id, 'event': event_id}).first()
        other = db.execute(text(sql.SQL_EVENT_CONFLICT),
                           {'dataset': dataset_id, 'event': event_id, 'hash': first[1]}).first()
        raise ValueError(f'事件编号 {event_id} 的内容冲突：CSV 第 {first[0]} 行与第 {other[0]} 行'
                         '不一致，请核对原始交接单。')
    slot = db.execute(text(sql.SQL_SLOT), {'dataset': dataset_id}).first()
    if slot:
        asset_id, day, sequence = slot
        rows = db.execute(text(sql.SQL_SLOT_ROWS),
                          {'dataset': dataset_id, 'asset': asset_id, 'day': str(day), 'seq': sequence}).all()
        (row_a, event_a), (row_b, event_b) = rows[0], rows[1]
        raise ValueError(f'工具 {asset_id} 在 {day} 的顺序号 {sequence} 重复'
                         f'（CSV 第 {row_a} 行 {event_a}、第 {row_b} 行 {event_b}）。')


def finalize(db, dataset):
    """收尾入库：集合校验、编排回放序号、按指纹去重。返回 (批次, 是否新建)。"""
    if dataset.status == 'ready':
        return dataset, False
    if dataset.status != 'importing':
        raise ValueError(dataset.error or '该批次入库未完成，请重新导入')
    if dataset.imported_rows == 0:
        raise ValueError('交接簿为空，请至少上传一行事件')
    _check_conflicts(db, dataset.id)
    db.execute(text(sql.SQL_REPLAY_ORDER), {'dataset': dataset.id})
    dataset.event_count = db.execute(text(sql.SQL_EVENT_COUNT), {'dataset': dataset.id}).scalar_one()
    dataset.row_count = dataset.imported_rows
    dataset.checksum = dataset.rolling_hash
    dataset.status = 'ready'
    try:
        with db.begin_nested():
            db.flush()
    except IntegrityError:
        existing = db.scalar(select(Dataset).where(Dataset.name == dataset.name,
                                                   Dataset.checksum == dataset.checksum,
                                                   Dataset.id != dataset.id,
                                                   Dataset.status == 'ready'))
        if existing is None:
            raise
        db.execute(delete(LedgerEvent).where(LedgerEvent.dataset_id == dataset.id))
        db.execute(delete(LedgerChunk).where(LedgerChunk.dataset_id == dataset.id))
        db.delete(dataset)
        return existing, False
    return dataset, True


def ingest_rows(db, name, filename, rows, actor):
    """一次性入库（小簿与演示数据同样走分批通道）。"""
    dataset = begin(db, name, filename, actor)
    for offset in range(0, len(rows), BATCH_SIZE):
        append_chunk(db, dataset, dataset.next_chunk, rows[offset:offset + BATCH_SIZE])
    return finalize(db, dataset)
