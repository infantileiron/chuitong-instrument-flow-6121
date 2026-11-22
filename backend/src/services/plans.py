import hashlib
import json

from fastapi import HTTPException

from src.constants.domain import Role, RunStatus
from src.models.db import one, rows
from src.services import batches
from src.services.batches import DEFAULT_BATCH_SIZE
from src.services.catalog import audit, catalog
from src.utils.crypto import canonical

RUN_FIELDS = '''r.id,r.plan_id,r.status,r.error,r.created_by,r.created_at,r.published_at,r.published_by,
    p.series,p.version,p.start_on,p.end_on,p.instrument'''


def add_plan(cursor, data, user):
    cursor.execute('''INSERT INTO plans(series,version,start_on,end_on,instrument,created_by)
        VALUES (%s,%s,%s,%s,%s,%s)''',
        (data.series, data.version, data.start_on, data.end_on, data.instrument, user['username']))
    plan_id = cursor.lastrowid
    audit(cursor, user['username'], 'plan_created', plan_id)
    return {**data.model_dump(), 'id': plan_id, 'created_by': user['username']}


def snapshot_for(cursor, plan):
    scope = 'effective_on BETWEEN %s AND %s AND (%s IS NULL OR instrument=%s)'
    params = (plan['start_on'], plan['end_on'], plan['instrument'], plan['instrument'])
    receipts = rows(cursor, f'SELECT * FROM receipts WHERE {scope} ORDER BY id', params)
    originals = rows(cursor, f'''SELECT o.* FROM receipts o JOIN (
        SELECT original_id FROM receipts WHERE {scope} AND original_id IS NOT NULL
        ) r ON r.original_id=o.id ORDER BY o.id''', params)
    snapshot = {**catalog(cursor), 'receipts': receipts, 'originals': originals}
    if plan['instrument']:
        snapshot['instruments'] = [item for item in snapshot['instruments'] if item['code'] == plan['instrument']]
    return canonical(snapshot)


def enqueue(cursor, plan_id, user, batch_size=DEFAULT_BATCH_SIZE):
    plan = one(cursor, 'SELECT * FROM plans WHERE id=%s', (plan_id,))
    if not plan:
        raise HTTPException(404, '方案版本不存在')
    snapshot_json = snapshot_for(cursor, plan)
    cursor.execute('INSERT INTO runs(plan_id,snapshot,created_by) VALUES (%s,%s,%s)',
                   (plan_id, snapshot_json, user['username']))
    run_id = cursor.lastrowid
    total = batches.create_batches(cursor, run_id, json.loads(snapshot_json), batch_size)
    audit(cursor, user['username'], 'run_queued', f'{run_id}:{total}')
    return {'id': run_id, 'status': RunStatus.QUEUED, 'total_batches': total, 'batch_size': batch_size}


def resume(cursor, run_id, user):
    run = one(cursor, 'SELECT id,status,published_at FROM runs WHERE id=%s', (run_id,))
    if not run or (user['role'] == Role.OBSERVER and not run['published_at']):
        raise HTTPException(404, '当前席位没有可阅的这份报告')
    if run['status'] in (RunStatus.DONE, RunStatus.FAILED):
        raise HTTPException(409, '终态运行无需接手；失败请修正凭据后重新运行')
    # 抢占与审计在 resume_run 自己的事务里完成，避免外层 REPEATABLE READ 读旧
    return batches.resume_run(run_id, actor=user['username'])


def list_plans(cursor, user):
    visibility = '' if user['role'] != Role.OBSERVER else (
        'WHERE EXISTS (SELECT 1 FROM runs r WHERE r.plan_id=p.id AND r.published_at IS NOT NULL)')
    return rows(cursor, f'SELECT p.* FROM plans p {visibility} ORDER BY p.series,p.version DESC')


def list_runs(cursor, user):
    visibility = '' if user['role'] != Role.OBSERVER else 'WHERE r.published_at IS NOT NULL'
    return rows(cursor, f'''SELECT {RUN_FIELDS},r.total_batches,r.batch_size,
        (SELECT COUNT(*) FROM run_batches b WHERE b.run_id=r.id AND b.status='done') AS done_batches
        FROM runs r JOIN plans p ON p.id=r.plan_id
        {visibility} ORDER BY r.id DESC LIMIT 100''')


def get_run(cursor, run_id, user):
    run = one(cursor, f'''SELECT {RUN_FIELDS}, r.snapshot, r.result, r.batch_size, r.total_batches,
        r.worker_epoch, r.heartbeat_at FROM runs r
        JOIN plans p ON p.id=r.plan_id WHERE r.id=%s''', (run_id,))
    if not run or (user['role'] == Role.OBSERVER and not run['published_at']):
        raise HTTPException(404, '当前席位没有可阅的这份报告')
    snapshot = json.loads(run.pop('snapshot'))
    run['snapshot_hash'] = hashlib.sha256(canonical(snapshot).encode()).hexdigest()
    run['catalog'] = {key: snapshot[key] for key in ('halls', 'instruments')}
    run['receipt_refs'] = [row['reference'] for row in snapshot['receipts']]
    run['result'] = json.loads(run['result']) if run['result'] else None
    run['progress'] = batches.progress(cursor, run_id)
    run['batch_rows'] = rows(cursor, '''SELECT batch_no,status,start_index,end_index,attempt,error,worker_id
        FROM run_batches WHERE run_id=%s ORDER BY batch_no''', (run_id,))
    return run, snapshot


def publish(cursor, run_id, user):
    run = one(cursor, 'SELECT status,published_at FROM runs WHERE id=%s FOR UPDATE', (run_id,))
    if not run:
        raise HTTPException(404, '运行记录不存在')
    if run['status'] != RunStatus.DONE:
        raise HTTPException(409, '只有核验完成的运行可以发布')
    if not run['published_at']:
        cursor.execute('UPDATE runs SET published_at=UTC_TIMESTAMP(),published_by=%s WHERE id=%s',
                       (user['username'], run_id))
        audit(cursor, user['username'], 'run_published', run_id)
    return get_run(cursor, run_id, user)[0]
