"""最小内存数据库，模拟 batches.py 用到的 MySQL 方言。

- 全局事务锁串行化所有连接，FOR UPDATE / SKIP LOCKED 退化为串行领取；
- UTC_TIMESTAMP() 映射为 naive UTC datetime；
- JSON 列以 canonical 字符串存储，读取时还原；
- UPDATE 的 rowcount 严格按命中行数返回，用于 fencing 断言。
"""
import datetime
import json
import re
import threading
from contextlib import contextmanager

from src.utils.crypto import canonical

_LOCK = threading.RLock()
JSON_COLUMNS = {
    'runs': {'snapshot', 'result'},
    'run_batches': {'partial', 'reversed_ids'},
}


def _utc():
    return datetime.datetime.utcnow()


def _norm(sql):
    return re.sub(r'\s+', ' ', sql).strip()


class FakeDB:
    def __init__(self):
        self.tables = {'runs': {}, 'run_batches': {}, 'plans': {}, 'audit_log': {}}
        self.auto = {'runs': 0, 'run_batches': 0, 'plans': 0, 'audit_log': 0}
        self.saved_reports = {}  # run_id -> 调用 save_report 的次数/载荷

    def reset(self):
        self.__init__()

    def insert(self, table, row):
        self.auto[table] += 1
        row = dict(row)
        row.setdefault('id', self.auto[table])
        if row['id'] > self.auto[table]:
            self.auto[table] = row['id']
        for column in JSON_COLUMNS.get(table, ()):
            if column in row and not isinstance(row[column], str):
                row[column] = canonical(row[column])
        self.tables[table][row['id']] = row
        return row['id']

    def get(self, table, row_id):
        return self.tables[table].get(row_id)

    def select(self, table, filters=None, order=None, limit=None):
        rows = [dict(row) for row in self.tables[table].values()]
        for key, value in (filters or {}).items():
            if isinstance(value, (list, tuple, set)):
                rows = [row for row in rows if row.get(key) in value]
            elif value is None:
                rows = [row for row in rows if row.get(key) is None]
            else:
                rows = [row for row in rows if row.get(key) == value]
        if order:
            rows.sort(key=order)
        if limit is not None:
            rows = rows[:limit]
        for row in rows:
            for column in JSON_COLUMNS.get(table, ()):
                if column in row and isinstance(row[column], str):
                    row[column] = json.loads(row[column])
        return rows


class FakeCursor:
    def __init__(self, db):
        self.db = db
        self._result = []
        self.rowcount = 0

    def execute(self, sql, params=()):
        self._result = []
        self.rowcount = 0
        dispatch(self, _norm(sql), params)

    def fetchone(self):
        return self._result[0] if self._result else None

    def fetchall(self):
        return self._result

    # -- 供 dispatch 使用的辅助 --
    def set_result(self, rows):
        self._result = rows


@contextmanager
def fake_transaction():
    with _LOCK:
        cursor = FakeCursor(_DB)
        yield cursor


def one(cursor, query, values=()):
    cursor.execute(query, values)
    return cursor.fetchone()


def rows(cursor, query, values=()):
    cursor.execute(query, values)
    return cursor.fetchall()


_DB = FakeDB()


def db():
    return _DB


# ---------------------------------------------------------------------------
# SQL 分发：覆盖 src/services/batches.py 的全部语句
# ---------------------------------------------------------------------------
def dispatch(cursor, sql, params):
    db = cursor.db
    p = list(params)

    if sql.startswith('SELECT status,worker_epoch FROM runs WHERE id='):
        row = db.get('runs', p[0])
        cursor.set_result([{k: row[k] for k in ('status', 'worker_epoch')}] if row else [])
        return

    if sql.startswith('SELECT * FROM runs WHERE id='):
        row = db.get('runs', p[0])
        cursor.set_result([dict(row)] if row else [])
        return

    if sql.startswith('SELECT snapshot FROM runs WHERE id='):
        row = db.get('runs', p[0])
        cursor.set_result([{'snapshot': json.loads(row['snapshot'])}] if row else [])
        return

    if sql.startswith("SELECT * FROM run_batches WHERE run_id=") and 'FOR UPDATE SKIP LOCKED' in sql:
        candidates = db.select('run_batches',
                               filters={'run_id': p[0], 'status': ('queued', 'running')},
                               order=lambda r: r['batch_no'], limit=1)
        cursor.set_result(candidates)
        return

    if sql.startswith('SELECT r.id FROM runs r WHERE r.status IN'):
        candidates = []
        for row in db.tables['runs'].values():
            if row['status'] not in ('queued', 'running'):
                continue
            has_pending = any(b['run_id'] == row['id'] and b['status'] in ('queued', 'running')
                              for b in db.tables['run_batches'].values())
            if has_pending:
                candidates.append(row)
        rank = {'running': 0, 'queued': 1}
        candidates.sort(key=lambda r: (rank[r['status']], r['id']))
        cursor.set_result([{'id': candidates[0]['id']}] if candidates else [])
        return

    if sql.startswith('SELECT partial,reversed_ids,batch_no FROM run_batches'):
        cursor.set_result(db.select('run_batches', filters={'run_id': p[0], 'status': 'done'},
                                    order=lambda r: r['batch_no']))
        return

    if sql.startswith('SELECT partial,reversed_ids FROM run_batches'):
        cursor.set_result(db.select('run_batches', filters={'run_id': p[0]}, order=lambda r: r['batch_no']))
        return

    if sql.startswith('SELECT COUNT(*) AS count FROM run_batches'):
        match = re.search(r"status IN \('queued','running'\)", sql)
        if match:
            count = sum(1 for row in db.tables['run_batches'].values()
                        if row['run_id'] == p[0] and row['status'] in ('queued', 'running'))
        else:
            count = sum(1 for row in db.tables['run_batches'].values() if row['run_id'] == p[0])
        cursor.set_result([{'count': count}])
        return

    if sql.startswith('SELECT r.id,r.plan_id,r.status,r.error,r.batch_size,r.total_batches'):
        run = db.get('runs', p[0])
        plan = db.get('plans', run['plan_id']) if run else None
        if not run:
            cursor.set_result([])
            return
        cursor.set_result([{**{k: run[k] for k in ('id', 'plan_id', 'status', 'error', 'batch_size',
                                                   'total_batches', 'worker_epoch', 'heartbeat_at')},
                            'series': plan['series'], 'version': plan['version']}])
        return

    if sql.startswith('SELECT status,COUNT(*) AS count FROM run_batches'):
        grouped = {}
        for row in db.tables['run_batches'].values():
            if row['run_id'] == p[0]:
                grouped[row['status']] = grouped.get(row['status'], 0) + 1
        cursor.set_result([{'status': status, 'count': count} for status, count in grouped.items()])
        return

    if sql.startswith('SELECT COALESCE(SUM(end_index-start_index),0) AS n FROM run_batches'):
        only_done = "status='done'" in sql
        total = sum(row['end_index'] - row['start_index']
                    for row in db.tables['run_batches'].values()
                    if row['run_id'] == p[0] and (not only_done or row['status'] == 'done'))
        cursor.set_result([{'n': total}])
        return

    if sql.startswith('SELECT batch_no,status,start_index,end_index,attempt,error,worker_id FROM run_batches'):
        cursor.set_result(db.select('run_batches', filters={'run_id': p[0]}, order=lambda r: r['batch_no']))
        return

    # ---------------- UPDATES ----------------
    if sql.startswith("UPDATE runs SET status='running',worker_epoch="):
        epoch, run_id = p
        row = db.get('runs', run_id)
        row['status'] = 'running'
        row['worker_epoch'] = epoch
        row['heartbeat_at'] = _utc()
        cursor.rowcount = 1
        return

    if sql.startswith("UPDATE run_batches SET status='running',worker_id="):
        worker_id, attempt, batch_id = p
        row = db.get('run_batches', batch_id)
        row.update(status='running', worker_id=worker_id, attempt=attempt, started_at=_utc(), error=None)
        cursor.rowcount = 1
        return

    if sql.startswith('UPDATE run_batches b JOIN runs r ON r.id=b.run_id'):
        batch = db.get('run_batches', p[-2])
        run = db.get('runs', batch['run_id'])
        if batch['status'] == 'running' and run['worker_epoch'] == p[-1]:
            if "b.status='done'" in sql:
                batch.update(status='done', partial=p[0], reversed_ids=p[1], finished_at=_utc())
            else:
                batch.update(status='failed', error=p[0], finished_at=_utc())
            cursor.rowcount = 1
        return

    if sql.startswith('UPDATE runs SET heartbeat_at=UTC_TIMESTAMP() WHERE id='):
        run_id, epoch = p
        run = db.get('runs', run_id)
        if run and run['worker_epoch'] == epoch:
            run['heartbeat_at'] = _utc()
            cursor.rowcount = 1
        return

    if sql.startswith("UPDATE runs SET status='failed'"):
        message, run_id, epoch = p
        run = db.get('runs', run_id)
        if run and run['worker_epoch'] == epoch:
            run.update(status='failed', error=message, heartbeat_at=None)
            cursor.rowcount = 1
        return

    if sql.startswith("UPDATE runs SET status='done',result="):
        result_json, run_id, epoch = p
        run = db.get('runs', run_id)
        if run and run['worker_epoch'] == epoch:
            run.update(status='done', result=result_json, error=None, heartbeat_at=None)
            cursor.rowcount = 1
        return

    if sql.startswith('UPDATE runs SET worker_epoch=worker_epoch+1'):
        run = db.get('runs', p[0])
        run['worker_epoch'] += 1
        run['heartbeat_at'] = None
        cursor.rowcount = 1
        return

    if sql.startswith("UPDATE run_batches SET status='queued',worker_id=''"):
        for row in db.tables['run_batches'].values():
            if row['run_id'] == p[0] and row['status'] == 'running':
                row.update(status='queued', worker_id='', started_at=None)
                cursor.rowcount += 1
        return

    if sql.startswith('INSERT INTO audit_log'):
        if 'run_completed' in sql:
            entry = {'actor': 'worker', 'action': 'run_completed', 'target': p[0]}
        else:
            entry = {'actor': p[0], 'action': p[1], 'target': p[2]}
        db.insert('audit_log', {**entry, 'created_at': _utc()})
        cursor.rowcount = 1
        return

    raise AssertionError(f'未实现的 SQL: {sql}')
