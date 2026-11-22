import json
import logging
import os
import uuid
from datetime import datetime

from src.constants.domain import RunStatus
from src.models.db import one, rows, transaction
from src.services.pivot import (LedgerMismatch, finalize, merge_batches, process_batch,
                                split_batches)
from src.services.reports import save_report
from src.utils.crypto import canonical

logger = logging.getLogger(__name__)

DEFAULT_BATCH_SIZE = int(os.environ.get('PIVOT_BATCH_SIZE', '5'))
# 超过该秒数没有心跳的运行视为僵死，可被另一个进程接手
STALE_AFTER_SECONDS = int(os.environ.get('RUN_STALE_SECONDS', '30'))


def create_batches(cursor, run_id, snapshot, batch_size):
    """按冻结快照固定切批并落库，返回批次总数。只能在创建运行的事务里调用一次。"""
    bounds = split_batches(snapshot['receipts'], batch_size) or [(0, 0)]
    for batch_no, (start, end) in enumerate(bounds, start=1):
        cursor.execute('''INSERT INTO run_batches(run_id,batch_no,start_index,end_index,status)
            VALUES (%s,%s,%s,%s,'queued')''', (run_id, batch_no, start, end))
    cursor.execute('UPDATE runs SET batch_size=%s,total_batches=%s WHERE id=%s',
                   (batch_size, len(bounds), run_id))
    return len(bounds)


def _worker_id():
    return f'{uuid.getnode():x}-{os.getpid()}'


def _claim_next_batch(cursor, run_id, worker_id):
    """行级锁领取该运行下一个未完成批次；返回 (batch, epoch)，没有则返回 (None, epoch)。"""
    run = one(cursor, 'SELECT status,worker_epoch FROM runs WHERE id=%s FOR UPDATE', (run_id,))
    if not run or run['status'] not in (RunStatus.QUEUED, RunStatus.RUNNING):
        return None, run['worker_epoch'] if run else 0
    batch = one(cursor, '''SELECT * FROM run_batches WHERE run_id=%s AND status IN ('queued','running')
        ORDER BY batch_no LIMIT 1 FOR UPDATE SKIP LOCKED''', (run_id,))
    if not batch:
        return None, run['worker_epoch']
    epoch = run['worker_epoch'] + 1 if run['status'] == RunStatus.QUEUED else run['worker_epoch']
    if run['status'] == RunStatus.QUEUED:
        cursor.execute("UPDATE runs SET status='running',worker_epoch=%s,heartbeat_at=UTC_TIMESTAMP() WHERE id=%s",
                       (epoch, run_id))
    attempt = batch['attempt'] + 1
    cursor.execute('''UPDATE run_batches SET status='running',worker_id=%s,attempt=%s,
        started_at=UTC_TIMESTAMP(),error=NULL WHERE id=%s''', (worker_id, attempt, batch['id']))
    return {**batch, 'attempt': attempt}, epoch


def _finish_batch(cursor, batch, epoch, partial, reversed_ids):
    """带 fencing 的条件提交：纪元已被接手者推进时，迟到的旧进程写入会被拒绝。"""
    cursor.execute('''UPDATE run_batches b JOIN runs r ON r.id=b.run_id
        SET b.status='done',b.partial=%s,b.reversed_ids=%s,b.finished_at=UTC_TIMESTAMP()
        WHERE b.id=%s AND b.status='running' AND r.worker_epoch=%s''',
                   (canonical(partial), canonical(reversed_ids), batch['id'], epoch))
    if cursor.rowcount != 1:
        raise StaleWorker(f"批次 {batch['batch_no']} 的写入已被新进程取代")
    cursor.execute('UPDATE runs SET heartbeat_at=UTC_TIMESTAMP() WHERE id=%s AND worker_epoch=%s',
                   (batch['run_id'], epoch))


def _fail_batch(cursor, batch, epoch, message):
    cursor.execute('''UPDATE run_batches b JOIN runs r ON r.id=b.run_id
        SET b.status='failed',b.error=%s,b.finished_at=UTC_TIMESTAMP()
        WHERE b.id=%s AND b.status='running' AND r.worker_epoch=%s''',
                   (message[:240], batch['id'], epoch))
    if cursor.rowcount == 1:
        cursor.execute('''UPDATE runs SET status='failed',error=%s,heartbeat_at=NULL
            WHERE id=%s AND worker_epoch=%s''', (message[:240], batch['run_id'], epoch))


class StaleWorker(RuntimeError):
    """当前进程的纪元已过期，整轮处理结果必须作废。"""


def resume_run(run_id, actor=None):
    """供 API 调用：显式接手一个中断/僵死的运行，沿用其冻结快照与已完成批次。

    - done/failed 的终态运行不可接手；
    - 仍在跑但心跳僵死的运行可抢占，worker_epoch 自增使旧进程 fencing 失效；
    - 排队中或正常运行中的运行幂等返回当前进度；
    - actor 给出时在同一事务内写审计，避免外层事务隔离级别造成读旧。
    """
    worker_id = _worker_id()
    with transaction() as cursor:
        run = one(cursor, 'SELECT * FROM runs WHERE id=%s FOR UPDATE', (run_id,))
        if not run:
            raise LookupError(run_id)
        if run['status'] in (RunStatus.DONE, RunStatus.FAILED):
            return progress(cursor, run_id)
        stale = run['heartbeat_at'] is None or (
            datetime.utcnow() - run['heartbeat_at']).total_seconds() > STALE_AFTER_SECONDS
        if stale and run['status'] == RunStatus.RUNNING:
            cursor.execute('''UPDATE runs SET worker_epoch=worker_epoch+1,heartbeat_at=NULL WHERE id=%s''', (run_id,))
            cursor.execute('''UPDATE run_batches SET status='queued',worker_id='',started_at=NULL
                WHERE run_id=%s AND status='running' ''', (run_id,))
            logger.warning('Run %s 被 %s 接手，旧纪元作废', run_id, worker_id)
            if actor:
                cursor.execute('INSERT INTO audit_log(actor,action,target) VALUES (%s,%s,%s)',
                               (actor, 'run_resumed', str(run_id)))
        return progress(cursor, run_id)


def progress(cursor, run_id):
    run = one(cursor, '''SELECT r.id,r.plan_id,r.status,r.error,r.batch_size,r.total_batches,r.worker_epoch,
        r.heartbeat_at,p.series,p.version FROM runs r JOIN plans p ON p.id=r.plan_id WHERE r.id=%s''', (run_id,))
    if not run:
        raise LookupError(run_id)
    stats = rows(cursor, '''SELECT status,COUNT(*) AS count FROM run_batches
        WHERE run_id=%s GROUP BY status''', (run_id,))
    by_status = {row['status']: row['count'] for row in stats}
    done_batches = by_status.get('done', 0)
    run['done_batches'] = done_batches
    run['running_batches'] = by_status.get('running', 0)
    run['failed_batches'] = by_status.get('failed', 0)
    run['queued_batches'] = by_status.get('queued', 0)
    total_receipts = one(cursor, 'SELECT COALESCE(SUM(end_index-start_index),0) AS n FROM run_batches WHERE run_id=%s',
                         (run_id,))['n']
    done_receipts = one(cursor, '''SELECT COALESCE(SUM(end_index-start_index),0) AS n FROM run_batches
        WHERE run_id=%s AND status='done' ''', (run_id,))['n']
    run['receipt_total'] = total_receipts
    run['receipts_done'] = done_receipts
    run['percent'] = round(100 * done_receipts / total_receipts, 1) if total_receipts else 100.0
    return run


def _as_json(value):
    return json.loads(value) if isinstance(value, str) else value


def _mark_run_failed(cursor, run_id, epoch, message):
    cursor.execute('''UPDATE runs SET status='failed',error=%s,heartbeat_at=NULL
        WHERE id=%s AND worker_epoch=%s''', (message[:240], run_id, epoch))
    if cursor.rowcount != 1:
        raise StaleWorker(f'运行 {run_id} 失败回写时发现纪元已过期')


def finalize_run(cursor, run_id, snapshot, epoch):
    """全部批次完成后的收口：合并同族同方向件数、算双向相抵、守恒校验并出报告。

    收口阶段的全局去重是跨 worker 并发处理时双冲销的最后一道防线；
    任何 LedgerMismatch 都会把运行置为 failed，且不写报告。
    """
    all_rows = rows(cursor, '''SELECT partial,reversed_ids FROM run_batches
        WHERE run_id=%s ORDER BY batch_no''', (run_id,))
    partials = [_as_json(row['partial']) for row in all_rows]
    reversed_lists = [_as_json(row['reversed_ids']) for row in all_rows]
    try:
        events, reversed_set = merge_batches(partials, reversed_lists)
        result = finalize(snapshot, events, reversed_set)
    except LedgerMismatch as exc:
        _mark_run_failed(cursor, run_id, epoch, str(exc))
        return None
    # 先在事务内把运行收口为 done（带 fencing），提交后再落 CSV，避免纪元过期时留下孤儿报告
    cursor.execute('''UPDATE runs SET status='done',result=%s,error=NULL,
        heartbeat_at=NULL WHERE id=%s AND worker_epoch=%s''',
                   (canonical(result), run_id, epoch))
    if cursor.rowcount != 1:
        raise StaleWorker(f'运行 {run_id} 收口时发现纪元已过期')
    cursor.execute("INSERT INTO audit_log(actor,action,target) VALUES ('worker','run_completed',%s)", (str(run_id),))
    return result


def write_report_after_commit(run_id, snapshot, result):
    """事务提交成功后再写 CSV，保证报告与 done 状态同生共死。"""
    save_report(run_id, snapshot, result)


def consume(worker_id=None):
    """领取一个可运行的运行并处理它的下一个批次。返回处理过的 run_id，空闲返回 None。

    优先选择已有进度的运行（交班场景），其次选择排队中的新运行。
    """
    worker_id = worker_id or _worker_id()
    with transaction() as cursor:
        # 只要运行未终态且还有未完成批次就可领取；FOR UPDATE SKIP LOCKED 保证
        # 同一运行不会被两个 worker 同时领取。僵死判断只用于显式抢占（resume_run）。
        run = one(cursor, '''SELECT r.id FROM runs r WHERE r.status IN ('queued','running')
            AND EXISTS (SELECT 1 FROM run_batches b WHERE b.run_id=r.id
                AND b.status IN ('queued','running'))
            ORDER BY FIELD(r.status,'running','queued'), r.id LIMIT 1 FOR UPDATE SKIP LOCKED''')
        if not run:
            return None
        run_id = run['id']
        batch, epoch = _claim_next_batch(cursor, run_id, worker_id)
        snapshot_raw = one(cursor, 'SELECT snapshot FROM runs WHERE id=%s', (run_id,))['snapshot']
        snapshot = json.loads(snapshot_raw) if isinstance(snapshot_raw, str) else snapshot_raw
    if not batch:
        return run_id

    with transaction() as cursor:
        done_rows = rows(cursor, '''SELECT partial,reversed_ids,batch_no FROM run_batches
            WHERE run_id=%s AND status='done' ORDER BY batch_no''', (run_id,))
    seen = set()
    for row in done_rows:
        seen.update(_as_json(row['reversed_ids']))
    try:
        partial, reversed_ids = process_batch(snapshot, batch['start_index'], batch['end_index'], seen)
    except LedgerMismatch as exc:
        with transaction() as cursor:
            _fail_batch(cursor, batch, epoch, str(exc))
        logger.warning('Run %s 批次 %s 核验失败: %s', run_id, batch['batch_no'], exc)
        return run_id

    try:
        with transaction() as cursor:
            _finish_batch(cursor, batch, epoch, partial, reversed_ids)
            remaining = one(cursor, '''SELECT COUNT(*) AS count FROM run_batches
                WHERE run_id=%s AND status IN ('queued','running')''', (run_id,))['count']
            if remaining:
                return run_id
            result = finalize_run(cursor, run_id, snapshot, epoch)
        # 事务提交成功后再写 CSV；fencing 失败时 result 不会产生，也就不会有孤儿报告
        if result is not None:
            write_report_after_commit(run_id, snapshot, result)
    except StaleWorker:
        logger.warning('Worker %s 的纪元已过期，放弃 run %s 的迟到写入', worker_id, run_id)
    return run_id
