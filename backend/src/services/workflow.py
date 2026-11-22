import hashlib
import uuid

from fastapi import HTTPException
from sqlalchemy import and_, or_, select

from src.config.settings import cache, engine
from src.constants.workflow import Status
from src.models.tables import (audit, cases, evidence, expansion_segments,
                               run_leases, runs)
from src.services import domain
from src.utils.encoding import canonical
from src.utils.time import utc_time

# A worker refreshes its lease while it works; a running attempt without a
# refresh for this many seconds is treated as abandoned and may be reclaimed.
STALE_SECONDS = 600


def log(conn, actor, action, reference):
    conn.execute(audit.insert().values(actor=actor, action=action, reference=str(reference), created_at=utc_time()))


def recent(table):
    with engine.connect() as conn:
        return [dict(row) for row in conn.execute(select(table).order_by(table.c.id.desc()).limit(100)).mappings()]


def found(conn, table, identity):
    row = conn.execute(select(table).where(table.c.id == identity)).mappings().first()
    if row is None:
        raise HTTPException(404, '找不到该记录')
    return dict(row)


def make_case(title, payload, actor):
    title = title.strip()
    if not title:
        raise HTTPException(422, '档案名称不能为空')
    try:
        if len(canonical(payload)) > 512 * 1024:
            raise ValueError('业务输入超过 512 KiB')
        if callable(getattr(domain, 'validate', None)):
            domain.validate(payload)
    except (ValueError, TypeError) as exc:
        raise HTTPException(422, str(exc)) from None
    with engine.begin() as conn:
        case_id = conn.execute(cases.insert().values(slug=uuid.uuid4().hex, title=title, payload=payload,
                               author=actor, created_at=utc_time())).inserted_primary_key[0]
        log(conn, actor, 'create_case', case_id)
        return found(conn, cases, case_id)


def enqueue(case_id, actor):
    with engine.begin() as conn:
        found(conn, cases, case_id)
        run_id = conn.execute(runs.insert().values(case_id=case_id, status=Status.QUEUED,
                              owner=actor, created_at=utc_time())).inserted_primary_key[0]
        log(conn, actor, 'enqueue', run_id)
        result = found(conn, runs, run_id)
    try:
        cache.lpush('worker:wake', 'queued')
        cache.ltrim('worker:wake', 0, 99)
    except Exception:
        # The durable database queue is polled even if Redis is temporarily unavailable.
        pass
    return result


def claim():
    active = select(run_leases.c.run_id).where(
        run_leases.c.run_id == runs.c.id,
        run_leases.c.heartbeat_at >= utc_time(-STALE_SECONDS))
    ready = or_(and_(runs.c.status == Status.QUEUED, ~active.exists()),
                and_(runs.c.status == Status.RUNNING, ~active.exists()))
    with engine.begin() as conn:
        run = conn.execute(select(runs.c.id, runs.c.case_id).where(ready).order_by(runs.c.id)
                           .limit(1).with_for_update(skip_locked=True)).first()
        if not run:
            return None
        token = str(uuid.uuid4())
        conn.execute(runs.update().where(runs.c.id == run.id).values(
            started_at=utc_time(), status=Status.RUNNING, attempt_token=token))
        conn.execute(run_leases.delete().where(run_leases.c.run_id == run.id))
        conn.execute(run_leases.insert().values(run_id=run.id, token=token, heartbeat_at=utc_time()))
        return run.id, token, found(conn, cases, run.case_id)['payload']


def heartbeat(run_id, token):
    """Refresh the lease.  Returns False once another attempt owns the run."""
    with engine.begin() as conn:
        count = conn.execute(run_leases.update().where(run_leases.c.run_id == run_id,
                              run_leases.c.token == token).values(heartbeat_at=utc_time())).rowcount
        current = conn.execute(select(runs.c.status, runs.c.attempt_token)
                               .where(runs.c.id == run_id)).first()
    return bool(count) and current is not None and current.status == Status.RUNNING \
        and current.attempt_token == token


def retry(run_id, actor):
    with engine.begin() as conn:
        run = found(conn, runs, run_id)
        if run['status'] != Status.FAILED:
            raise HTTPException(409, '只有失败的执行可以重试')
        # Keep durable segment checkpoints; the retried attempt resumes after
        # the last completed segment instead of expanding the batch again.
        # Clearing the attempt token also revokes a lingering attempt: its next
        # heartbeat fails and its late completion is rejected by the token guard.
        conn.execute(runs.update().where(runs.c.id == run_id).values(
            status=Status.QUEUED, started_at=None, finished_at=None,
            error=None, attempt_token=None))
        conn.execute(run_leases.delete().where(run_leases.c.run_id == run_id))
        log(conn, actor, 'retry', run_id)
        result = found(conn, runs, run_id)
    try:
        cache.lpush('worker:wake', 'queued')
        cache.ltrim('worker:wake', 0, 99)
    except Exception:
        pass
    return result


def finish(run_id, token, result=None, error=None):
    digest = hashlib.sha256(canonical(result)).hexdigest() if result is not None else None
    status = Status.FAILED if error else Status.COMPLETE
    with engine.begin() as conn:
        count = conn.execute(runs.update().where(runs.c.id == run_id,
                        runs.c.attempt_token == token, runs.c.status == Status.RUNNING)
                        .values(status=status, finished_at=utc_time(), error=error)).rowcount
        if not count:
            return
        if result is not None:
            conn.execute(evidence.insert().values(run_id=run_id, report=result, digest=digest))
            conn.execute(expansion_segments.delete().where(expansion_segments.c.run_id == run_id))
        conn.execute(run_leases.delete().where(run_leases.c.run_id == run_id))
        log(conn, 'worker', status.value, run_id)


def interrup(run_id, token, actor='worker'):
    """Mark a run failed but keep its segment checkpoints for a retry."""
    with engine.begin() as conn:
        count = conn.execute(runs.update().where(runs.c.id == run_id,
                        runs.c.attempt_token == token, runs.c.status == Status.RUNNING)
                        .values(status=Status.FAILED, finished_at=utc_time(),
                                error='执行中断，已完成的分段已保存，可重试继续。')).rowcount
        if count:
            conn.execute(run_leases.delete().where(run_leases.c.run_id == run_id))
            log(conn, actor, Status.FAILED.value, run_id)


def review(identity, note, actor):
    note = note.strip()
    if not note:
        raise HTTPException(422, '复核意见不能为空')
    with engine.begin() as conn:
        found(conn, evidence, identity)
        count = conn.execute(evidence.update().where(evidence.c.id == identity, evidence.c.reviewed_at.is_(None))
                            .values(review_note=note, reviewer=actor, reviewed_at=utc_time())).rowcount
        if count != 1:
            raise HTTPException(409, '该证据已经复核')
        log(conn, actor, 'review', identity)
        return found(conn, evidence, identity)
