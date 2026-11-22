"""batches 编排测试：续跑、纪元 fencing、跨批双冲销失败、进度与并发领取。"""
import datetime
import json
import threading

from tests import harness  # noqa: F401  安装 stub 后再导入业务模块
import src.services.batches as batches
from src.constants.domain import ReceiptKind
from src.models import db as db_module
from src.services.pivot import LedgerMismatch, calculate
from src.utils.crypto import canonical

from tests import fakedb
from tests.fakedb import db, fake_transaction, one, rows
from tests.harness import demo_snapshot, make_receipt, make_snapshot


def as_obj(value):
    return json.loads(value) if isinstance(value, str) else value


def result_matches(run, snapshot):
    return canonical(as_obj(run['result'])) == canonical(calculate(snapshot))

# 用内存假库替换真实 PyMySQL 入口
batches.transaction = fake_transaction
batches.one = one
batches.rows = rows
db_module.transaction = fake_transaction

SAVED_REPORTS = []


def _fake_save_report(run_id, snapshot, result):
    SAVED_REPORTS.append((run_id, canonical(result)))


batches.save_report = _fake_save_report


def seed_run(snapshot, batch_size, status='queued'):
    SAVED_REPORTS.clear()
    database = db()
    database.reset()
    plan_id = database.insert('plans', {'series': '十一月上半月调借', 'version': 2,
                                        'start_on': '2026-11-01', 'end_on': '2026-11-15',
                                        'instrument': None, 'created_by': 'analyst'})
    run_id = database.insert('runs', {
        'plan_id': plan_id, 'status': status, 'snapshot': snapshot, 'result': None,
        'error': None, 'created_by': 'analyst', 'published_at': None, 'published_by': None,
        'batch_size': batch_size, 'total_batches': 0, 'worker_epoch': 0, 'heartbeat_at': None,
    })
    bounds = batches.split_batches(snapshot['receipts'], batch_size) or [(0, 0)]
    for batch_no, (start, end) in enumerate(bounds, start=1):
        database.insert('run_batches', {
            'run_id': run_id, 'batch_no': batch_no, 'start_index': start, 'end_index': end,
            'status': 'queued', 'partial': None, 'reversed_ids': None, 'error': None,
            'worker_id': '', 'attempt': 0, 'started_at': None, 'finished_at': None,
        })
    run = database.get('runs', run_id)
    run['total_batches'] = len(bounds)
    return run_id, len(bounds)


def drain(run_id):
    calls = 0
    while calls < 50:
        claimed = batches.consume(worker_id=f'worker-{calls % 2}')
        if claimed is None:
            break
        calls += 1
    return calls


def test_full_run_batched_equals_reference():
    snapshot = demo_snapshot()
    run_id, total = seed_run(snapshot, batch_size=3)
    assert total == 5
    calls = drain(run_id)
    assert calls == total  # 每个 consume 恰好推进一个批次
    run = db().get('runs', run_id)
    assert run['status'] == 'done'
    assert result_matches(run, snapshot)
    assert all(b['status'] == 'done' for b in db().select('run_batches', {'run_id': run_id}))
    assert SAVED_REPORTS == [(run_id, canonical(as_obj(run['result'])))]  # 报告只在收口时写一次


def test_resume_after_interruption_keeps_finished_batches():
    snapshot = demo_snapshot()
    run_id, _ = seed_run(snapshot, batch_size=2)
    # 先完成两个批次
    batches.consume(worker_id='worker-a')
    batches.consume(worker_id='worker-a')
    run = db().get('runs', run_id)
    assert run['status'] == 'running'
    finished_partial = db().select('run_batches', {'run_id': run_id, 'status': 'done'},
                                   order=lambda r: r['batch_no'])[0]['partial']
    # 模拟第三个批次处理到一半进程被杀：批次停在 running、心跳僵死
    stuck = db().select('run_batches', {'run_id': run_id, 'status': 'queued'},
                        order=lambda r: r['batch_no'])[0]
    stuck['status'] = 'running'
    stuck['worker_id'] = 'dead-worker'
    run['heartbeat_at'] = datetime.datetime.utcnow() - datetime.timedelta(seconds=300)

    progress = batches.resume_run(run_id)
    assert progress['status'] == 'running'
    assert run['worker_epoch'] == 2  # 首个批次领取时纪元已变为 1，接手再自增一次
    assert db().get('run_batches', stuck['id'])['status'] == 'queued'
    # 已完成批次的中间态原样保留，没有从头重算
    assert canonical(as_obj(db().get('run_batches', 1)['partial'])) == canonical(finished_partial)

    drain(run_id)
    run = db().get('runs', run_id)
    assert run['status'] == 'done'
    assert result_matches(run, snapshot)
    assert all(b['attempt'] >= 1 for b in db().select('run_batches', {'run_id': run_id}))


def test_stale_run_is_claimed_without_explicit_resume():
    snapshot = demo_snapshot()
    run_id, _ = seed_run(snapshot, batch_size=4, status='running')
    run = db().get('runs', run_id)
    run['worker_epoch'] = 1
    run['heartbeat_at'] = datetime.datetime.utcnow() - datetime.timedelta(seconds=300)
    claimed = batches.consume(worker_id='worker-b')
    assert claimed == run_id  # 僵死运行可被直接接手
    drain(run_id)
    assert db().get('runs', run_id)['status'] == 'done'


def test_fencing_rejects_late_stale_worker():
    snapshot = demo_snapshot()
    run_id, _ = seed_run(snapshot, batch_size=2)
    with fake_transaction() as cursor:
        batch, epoch = batches._claim_next_batch(cursor, run_id, 'old-worker')
    assert epoch == 1
    # 旧进程僵死：心跳过期后新进程接手，纪元推进、运行中的批次回队
    db().get('runs', run_id)['heartbeat_at'] = datetime.datetime.utcnow() - datetime.timedelta(seconds=300)
    batches.resume_run(run_id)
    assert db().get('runs', run_id)['worker_epoch'] == 2
    # 旧进程带着过期纪元（1）迟到提交，必须被拒
    try:
        with fake_transaction() as cursor:
            batches._finish_batch(cursor, batch, epoch, {'SHENG': {}}, [])
    except batches.StaleWorker:
        pass
    else:
        raise AssertionError('过期纪元的批次提交应当被 fencing 拒绝')
    # 新进程正常跑完
    drain(run_id)
    run = db().get('runs', run_id)
    assert run['status'] == 'done'
    final_result = canonical(as_obj(run['result']))
    # 旧进程迟到的收口写入也不能覆盖新结果
    with fake_transaction() as cursor:
        cursor.execute("""UPDATE runs SET status='done',result=%s,error=NULL,
            heartbeat_at=NULL WHERE id=%s AND worker_epoch=%s""",
                       (canonical({'tampered': True}), run_id, epoch))
        assert cursor.rowcount == 0
    assert canonical(as_obj(db().get('runs', run_id)['result'])) == final_result


def test_double_reversal_across_batches_fails_and_blocks_report():
    loan = make_receipt('L-1', ReceiptKind.LOAN, 'CT', 'YH', 'SHENG', 3, '2026-11-02')
    r1 = make_receipt('CX-1', ReceiptKind.REVERSAL, 'CT', 'YH', 'SHENG', 3, '2026-11-03', original_id=1)
    r2 = make_receipt('CX-2', ReceiptKind.REVERSAL, 'CT', 'YH', 'SHENG', 3, '2026-11-09', original_id=1)
    snapshot = make_snapshot([loan, r1, r2])
    run_id, _ = seed_run(snapshot, batch_size=1)  # 每张凭据一批，双冲销必然跨批
    assert batches.consume(worker_id='w') == run_id  # 签领
    assert batches.consume(worker_id='w') == run_id  # 第一次冲销，成功
    assert batches.consume(worker_id='w') == run_id  # 第二次冲销，核验失败并把运行置为终态
    assert batches.consume(worker_id='w') is None    # 终态运行不再被领取
    run = db().get('runs', run_id)
    assert run['status'] == 'failed'
    assert '重复冲销' in run['error']
    assert as_obj(run['result']) is None
    assert SAVED_REPORTS == []  # 算全前不出报告
    failed_batch = next(b for b in db().select('run_batches', {'run_id': run_id}) if b['status'] == 'failed')
    assert '重复冲销' in failed_batch['error']


def test_progress_visibility_and_gating():
    snapshot = demo_snapshot()
    run_id, total = seed_run(snapshot, batch_size=2)
    with fake_transaction() as cursor:
        progress = batches.progress(cursor, run_id)
    assert progress['percent'] == 0.0
    assert progress['done_batches'] == 0 and progress['receipts_done'] == 0
    assert progress['receipt_total'] == len(snapshot['receipts'])
    for _ in range(2):
        batches.consume(worker_id='w')
    with fake_transaction() as cursor:
        progress = batches.progress(cursor, run_id)
    assert 0 < progress['percent'] < 100
    assert progress['done_batches'] == 2
    assert as_obj(db().get('runs', run_id)['result']) is None  # 未完成不出报告
    drain(run_id)
    with fake_transaction() as cursor:
        progress = batches.progress(cursor, run_id)
    assert progress['percent'] == 100.0
    assert progress['status'] == 'done'


def test_resume_is_idempotent_on_terminal_states():
    snapshot = demo_snapshot()
    run_id, _ = seed_run(snapshot, batch_size=5)
    drain(run_id)
    progress = batches.resume_run(run_id)  # done 不报错、不改写
    assert progress['status'] == 'done'
    assert db().get('runs', run_id)['worker_epoch'] == 1


def test_finalize_guard_catches_cross_worker_duplicate():
    """两个批次由不同 worker 并发完成、各自记录了同一原单 id，收口全局防护必须判失败。"""
    snapshot = demo_snapshot()
    run_id, _ = seed_run(snapshot, batch_size=2)
    # 先领取一次，把运行推进到 running、纪元置为 1
    batches.consume(worker_id='w0')
    assert db().get('runs', run_id)['worker_epoch'] == 1
    # 手动把全部批次置为 done，并在前两批的 reversed_ids 里埋入重复原单 id（绕过逐批 seen）
    actual = sorted((row for row in db().tables['run_batches'].values() if row['run_id'] == run_id),
                    key=lambda r: r['batch_no'])
    actual[0].update(status='done', partial={}, reversed_ids=[7], worker_id='w1', attempt=1)
    actual[1].update(status='done', partial={}, reversed_ids=[7], worker_id='w2', attempt=1)
    for row in actual[2:]:
        row.update(status='done', partial={}, reversed_ids=[], worker_id='w1', attempt=1)
    before = len(SAVED_REPORTS)
    with fake_transaction() as cursor:
        result = batches.finalize_run(cursor, run_id, snapshot, epoch=1)
    assert result is None
    run = db().get('runs', run_id)
    assert run['status'] == 'failed'
    assert '冲销两次' in run['error']
    assert len(SAVED_REPORTS) == before  # 失败不写报告


def test_resume_writes_audit_and_is_idempotent_when_healthy():
    snapshot = demo_snapshot()
    run_id, _ = seed_run(snapshot, batch_size=4)
    batches.consume(worker_id='w')
    # 心跳新鲜时接手是幂等的：不推进纪元、不写审计
    epoch_before = db().get('runs', run_id)['worker_epoch']
    progress = batches.resume_run(run_id, actor='analyst')
    assert progress['status'] == 'running'
    assert db().get('runs', run_id)['worker_epoch'] == epoch_before
    assert db().select('audit_log') == []
    # 心跳僵死后接手：纪元自增、批次回队、审计留痕
    db().get('runs', run_id)['heartbeat_at'] = datetime.datetime.utcnow() - datetime.timedelta(seconds=300)
    batches.resume_run(run_id, actor='analyst')
    assert db().get('runs', run_id)['worker_epoch'] == epoch_before + 1
    logs = db().select('audit_log')
    assert len(logs) == 1 and logs[0]['action'] == 'run_resumed' and logs[0]['actor'] == 'analyst'


def test_empty_snapshot_run_completes_with_placeholder_batch():
    snapshot = make_snapshot([])
    run_id, total = seed_run(snapshot, batch_size=5)
    assert total == 1  # 空快照保留一个 (0,0) 占位批，运行仍可被领取和收口
    calls = 0
    while batches.consume(worker_id='w') is not None:
        calls += 1
        assert calls < 5
    run = db().get('runs', run_id)
    assert run['status'] == 'done'
    assert as_obj(run['result'])['receipt_count'] == 0
    assert len(SAVED_REPORTS) == 1
    with fake_transaction() as cursor:
        assert batches.progress(cursor, run_id)['percent'] == 100.0


def test_concurrent_workers_drain_once():
    snapshot = demo_snapshot()
    run_id, total = seed_run(snapshot, batch_size=1)
    errors = []

    def worker():
        try:
            for _ in range(20):
                if batches.consume(worker_id=f'thread-{threading.get_ident()}') is None:
                    return
        except Exception as exc:  # pragma: no cover - 并发断言失败路径
            errors.append(exc)

    threads = [threading.Thread(target=worker) for _ in range(3)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert not errors
    run = db().get('runs', run_id)
    assert run['status'] == 'done'
    assert len([b for b in db().select('run_batches', {'run_id': run_id}) if b['status'] == 'done']) == total
    assert len(SAVED_REPORTS) == 1
    assert result_matches(run, snapshot)
