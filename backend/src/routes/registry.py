from urllib.parse import quote
from fastapi import APIRouter, Depends, HTTPException, Query, Response
from sqlalchemy import select
from sqlalchemy.orm import Session
from src.config.database import connection
from src.constants.enums import Role
from src.controllers.auth import allow, current
from src.domain.spec import SPEC
from src.models.identity import User
from src.models.source import Dataset, LedgerEvent
from src.schemas.requests import ImportBegin, ImportChunk, Upload
from src.services.audit import record
from src.services.datasets import template
from src.services.ingest import append_chunk, begin, finalize, ingest_rows, progress
from src.services.validation import parse_rows
from src.utils import views

router = APIRouter()


@router.get('/spec')
def spec():
    return {key: value for key, value in SPEC.items() if key != 'fixture'}


@router.get('/template')
def sample(actor: User = Depends(current)):
    filename = quote(SPEC['sample_name'])
    return Response(template(), media_type='text/csv; charset=utf-8',
                    headers={'Content-Disposition': f"attachment; filename*=UTF-8''{filename}"})


@router.get('/datasets')
def datasets(db: Session = Depends(connection), actor: User = Depends(current)):
    return [views.dataset(item) for item in db.scalars(select(Dataset).order_by(Dataset.id.desc()).limit(100))]


@router.get('/datasets/{dataset_id}')
def details(dataset_id: int, offset: int = Query(0, ge=0), limit: int = Query(16, ge=1, le=100),
            db: Session = Depends(connection), actor: User = Depends(current)):
    item = db.get(Dataset, dataset_id)
    if not item:
        raise HTTPException(404, '找不到该数据批次')
    data = views.dataset(item)
    if item.status != 'ready':
        # 入库未完成（或失败）的批次只回报进度，不供查阅明细
        return {**data, 'rows': []}
    rows = db.scalars(select(LedgerEvent).where(LedgerEvent.dataset_id == dataset_id)
                      .order_by(LedgerEvent.id).offset(offset).limit(limit)).all()
    return {**data, 'rows': [{'event_id': row.event_id, 'asset_id': row.asset_id,
                              'asset_name': row.asset_name, 'event_date': row.event_date.isoformat(),
                              'sequence': row.sequence, 'event_type': row.event_type} for row in rows]}


@router.post('/datasets', status_code=201)
def upload(body: Upload, db: Session = Depends(connection),
           actor: User = Depends(allow(Role.ADMIN, Role.ANALYST))):
    rows = parse_rows(body.csv_text)
    item, created = ingest_rows(db, body.name, body.filename, rows, actor.username)
    if created:
        record(db, actor.username, 'dataset.imported', item.id, rows=item.row_count)
    db.commit()
    return {**views.dataset(item), 'created': created}


def require_import(db, import_id, lock=False):
    item = db.get(Dataset, import_id, with_for_update=lock)
    if not item:
        raise HTTPException(404, '找不到该导入批次')
    return item


@router.post('/datasets/imports', status_code=201)
def import_begin(body: ImportBegin, db: Session = Depends(connection),
                 actor: User = Depends(allow(Role.ADMIN, Role.ANALYST))):
    item = begin(db, body.name, body.filename, actor.username)
    record(db, actor.username, 'dataset.import.begin', item.id, filename=item.filename)
    db.commit()
    return progress(item)


@router.get('/datasets/imports/{import_id}')
def import_progress(import_id: int, db: Session = Depends(connection), actor: User = Depends(current)):
    return progress(require_import(db, import_id))


@router.post('/datasets/imports/{import_id}/chunks')
def import_chunk(import_id: int, body: ImportChunk, db: Session = Depends(connection),
                 actor: User = Depends(allow(Role.ADMIN, Role.ANALYST))):
    item = require_import(db, import_id, lock=True)
    state = append_chunk(db, item, body.index, body.rows)
    db.commit()
    return state


@router.post('/datasets/imports/{import_id}/finish')
def import_finish(import_id: int, db: Session = Depends(connection),
                  actor: User = Depends(allow(Role.ADMIN, Role.ANALYST))):
    item = require_import(db, import_id, lock=True)
    try:
        item, created = finalize(db, item)
    except ValueError as error:
        if item.status == 'importing':
            item.status = 'failed'
            item.error = str(error)[:500]
            record(db, actor.username, 'dataset.import.failed', item.id, reason=item.error)
            db.commit()
        raise
    if created:
        record(db, actor.username, 'dataset.imported', item.id,
               rows=item.row_count, events=item.event_count)
    db.commit()
    return {**views.dataset(item), 'created': created}
