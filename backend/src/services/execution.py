import logging
from datetime import date, timedelta
from sqlalchemy import and_, delete, or_, select
from src.config.database import Session
from src.constants.enums import Status
from src.domain.replay import BATCH_SIZE, AssetState, Event, Replay, build_result
from src.domain.spec import SPEC
from src.models.jobs import Delivery, Run, RunAssetState, RunCheckpoint, Schedule
from src.models.source import Dataset, LedgerEvent
from src.services.audit import record
from src.utils.exports import write_report
from src.utils.time import now

log = logging.getLogger(__name__)

LEASE = timedelta(minutes=3)


def materialize_due():
    if not SPEC.get('scheduled'):
        return
    stamp = now()
    with Session.begin() as db:
        schedules = db.scalars(select(Schedule).where(Schedule.enabled.is_(True), Schedule.next_at <= stamp)
                                .order_by(Schedule.next_at).limit(20).with_for_update(skip_locked=True))
        for item in schedules:
            due = item.next_at
            dataset_id = item.dataset_id
            params = dict(item.params)
            if item.repeat_daily:
                elapsed_days = (stamp - due).days
                due += timedelta(days=elapsed_days)
                period_key = SPEC['schedule_date_param']
                report_day = date.fromisoformat(params[period_key]) + timedelta(days=elapsed_days)
                params[period_key] = report_day.isoformat()
                item.params = {**params, period_key: (report_day + timedelta(days=1)).isoformat()}
                dataset_id = db.scalar(select(Dataset.id).where(Dataset.status == 'ready')
                                       .order_by(Dataset.id.desc()).limit(1)) or dataset_id
                item.next_at = due + timedelta(days=1)
            else:
                item.enabled = False
            db.add(Run(dataset_id=dataset_id, schedule_id=item.id, scheduled_at=due,
                       name=item.name, params=params, author=item.author))


def claim():
    stamp = now()
    with Session.begin() as db:
        query = select(Run).where(or_(Run.status == Status.QUEUED,
                                      and_(Run.status == Status.RUNNING, Run.lease_until < stamp)))
        job = db.scalar(query.order_by(Run.id).limit(1).with_for_update(skip_locked=True))
        if job is None:
            return None
        if job.attempt >= 3:
            job.status = Status.FAILED
            job.error = '后台进程多次中断，请发起新的运行'
            job.finished_at = stamp
            record(db, 'worker', 'analysis.failed', job.id, reason=job.error)
            return None
        job.status = Status.RUNNING
        job.attempt += 1
        job.lease_until = stamp + LEASE
        return job.id, job.attempt, job.dataset_id, job.params


def _event_page(db, dataset_id, after, cutoff):
    """按回放序号取下一批，最多 BATCH_SIZE 笔。"""
    return db.scalars(select(LedgerEvent)
                      .where(LedgerEvent.dataset_id == dataset_id,
                             LedgerEvent.replay_order > after,
                             LedgerEvent.event_date <= cutoff)
                      .order_by(LedgerEvent.replay_order)
                      .limit(BATCH_SIZE)).all()


def _to_event(row):
    return Event(row.event_id, row.asset_id, row.asset_name,
                 row.event_date, row.sequence, row.event_type)


def _to_state(row):
    last = Event(row.last_event_id, row.asset_id, row.asset_name,
                 row.last_event_date, row.last_sequence, row.last_event_type)
    return AssetState(row.asset_name, row.status, last)


def _persist_states(db, run_id, existing, states, asset_ids):
    for asset_id in asset_ids:
        state = states.get(asset_id)
        if state is None:
            continue
        row = existing.get(asset_id)
        if row is None:
            db.add(RunAssetState(run_id=run_id, asset_id=asset_id, asset_name=state.name,
                                 status=state.status, last_event_date=state.last_event.event_date,
                                 last_sequence=state.last_event.sequence,
                                 last_event_type=state.last_event.event_type,
                                 last_event_id=state.last_event.event_id))
        else:
            row.asset_name = state.name
            row.status = state.status
            row.last_event_date = state.last_event.event_date
            row.last_sequence = state.last_event.sequence
            row.last_event_type = state.last_event.event_type
            row.last_event_id = state.last_event.event_id


def _state_pages(db, run_id):
    """按工具编号分页供给期末资产快照，单批不超过 BATCH_SIZE。"""
    cursor = ''
    while True:
        rows = db.scalars(select(RunAssetState).where(RunAssetState.run_id == run_id,
                                                      RunAssetState.asset_id > cursor)
                          .order_by(RunAssetState.asset_id).limit(BATCH_SIZE)).all()
        if not rows:
            return
        for row in rows:
            yield row.asset_id, _to_state(row)
        cursor = rows[-1].asset_id


def _deliver(db, job, result, failure):
    plan = db.get(Schedule, job.schedule_id)
    ready = not failure and result.get('ready', True)
    detail = '已保存 CSV 到站内收件箱' if ready else (failure or result['headline'])
    receipt = Delivery(run_id=job.id, recipient=plan.recipient,
                       state='delivered' if ready else 'blocked', detail=detail[:500])
    db.add(receipt)
    record(db, 'worker', f'delivery.{receipt.state}', job.id, recipient=plan.recipient)


def _cleanup(db, job_id):
    checkpoint = db.get(RunCheckpoint, job_id)
    if checkpoint:
        db.delete(checkpoint)
    db.execute(delete(RunAssetState).where(RunAssetState.run_id == job_id))


def _fail(job_id, attempt, message):
    with Session.begin() as db:
        job = db.get(Run, job_id, with_for_update=True)
        if job is None or job.attempt != attempt or job.status != Status.RUNNING:
            return
        job.lease_until = None
        job.finished_at = now()
        job.status = Status.FAILED
        job.error = message
        record(db, 'worker', 'analysis.failed', job.id, attempt=attempt, reason=message)
        _cleanup(db, job_id)
        if job.schedule_id:
            _deliver(db, job, None, message)


def execute(lease, on_batch=None):
    """分批回放整簿：每批提交一次断点，中断后从原进度续跑，重试不重复计数。"""
    job_id, attempt, dataset_id, _params = lease
    try:
        while True:
            with Session.begin() as db:
                job = db.get(Run, job_id, with_for_update=True)
                if job is None or job.attempt != attempt or job.status != Status.RUNNING:
                    return
                dataset = db.get(Dataset, dataset_id)
                if dataset is None or dataset.status != 'ready':
                    raise ValueError('数据批次尚未完成入库，无法盘点')
                start = date.fromisoformat(job.params['start_date'])
                cutoff = date.fromisoformat(job.params['as_of_date'])
                checkpoint = db.get(RunCheckpoint, job_id)
                replay = (Replay(start, cutoff) if checkpoint is None
                          else Replay.restore(start, cutoff, checkpoint.payload))
                rows = _event_page(db, dataset_id, checkpoint.cursor if checkpoint else 0, cutoff)
                if not rows:
                    result = build_result(replay, _state_pages(db, job_id))
                    export = write_report(job_id, attempt, result)
                    job.lease_until = None
                    job.finished_at = now()
                    job.status = Status.COMPLETE
                    job.result = result
                    job.export_file = export
                    job.error = None
                    _cleanup(db, job_id)
                    record(db, 'worker', f'analysis.{job.status}', job.id, attempt=attempt)
                    if job.schedule_id:
                        _deliver(db, job, result, None)
                    return
                asset_ids = {row.asset_id for row in rows}
                existing = {row.asset_id: row for row in db.scalars(
                    select(RunAssetState).where(RunAssetState.run_id == job_id,
                                                RunAssetState.asset_id.in_(asset_ids)))}
                states = {asset_id: _to_state(row) for asset_id, row in existing.items()}
                replay.consume([_to_event(row) for row in rows], states)
                _persist_states(db, job_id, existing, states, asset_ids)
                payload = replay.dump()
                if checkpoint is None:
                    db.add(RunCheckpoint(run_id=job_id, cursor=rows[-1].replay_order,
                                         processed=len(rows), payload=payload))
                else:
                    checkpoint.cursor = rows[-1].replay_order
                    checkpoint.processed += len(rows)
                    checkpoint.payload = payload
                job.lease_until = now() + LEASE
            if on_batch:
                on_batch()
    except ValueError as error:
        _fail(job_id, attempt, str(error)[:500])
    except Exception:
        # 进程或连接中断：保留断点，租约到期后由后台从原进度续跑。
        log.exception('Analysis %s interrupted; will resume from checkpoint', job_id)
