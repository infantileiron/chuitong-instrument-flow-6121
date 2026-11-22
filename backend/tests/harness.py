"""在没有 pymysql / redis / fastapi 的本地环境中，用 stub 注入被测模块。

测试只依赖标准库，覆盖：
- services/pivot.py：分批核算原语与一次算完的恒等性、跨批双冲销、伪造签名；
- services/batches.py：worker 领取/续跑/抢占 fencing、进度统计、收口出报告。

通过 `python -m tests.run_all` 或 `python tests/run_all.py` 执行。
"""
import datetime
import hashlib
import hmac
import json
import os
import sys
import types
from pathlib import Path

BACKEND = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BACKEND))

LEDGER_SECRET = b'test-ledger-secret'


def _install_stubs():
    # ---------- src.config.settings stub ----------
    config = types.ModuleType('src.config')
    settings = types.ModuleType('src.config.settings')
    settings.JWT_SECRET = 'test-jwt-secret'
    settings.LEDGER_SECRET = LEDGER_SECRET

    class _FakeRedis:
        def ping(self):
            return True

    settings.CACHE = _FakeRedis()
    config.settings = settings

    pymysql = types.ModuleType('pymysql')
    cursors = types.ModuleType('pymysql.cursors')

    class DictCursor:
        pass

    cursors.DictCursor = DictCursor
    pymysql.cursors = cursors
    pymysql.connect = lambda *a, **k: None

    redis_mod = types.ModuleType('redis')
    redis_mod.Redis = _FakeRedis

    fastapi = types.ModuleType('fastapi')

    class HTTPException(Exception):
        def __init__(self, status_code=400, detail=''):
            super().__init__(detail)
            self.status_code = status_code
            self.detail = detail

    fastapi.HTTPException = HTTPException

    for name, module in {
        'pymysql': pymysql, 'pymysql.cursors': cursors, 'redis': redis_mod,
        'fastapi': fastapi, 'src.config': config, 'src.config.settings': settings,
    }.items():
        sys.modules[name] = module


_install_stubs()

from src.constants.domain import ReceiptKind  # noqa: E402
from src.utils.crypto import canonical, receipt_signature  # noqa: E402


def sign(row):
    row['signature'] = receipt_signature(row)
    return row


def make_receipt(reference, kind, source, destination, instrument, quantity, effective_on,
                 original_id=None, signer='analyst', note='测试凭据'):
    return sign({
        'id': None,  # 由调用方在构造快照时编号
        'reference': reference,
        'kind': kind,
        'original_id': original_id,
        'source': source,
        'destination': destination,
        'instrument': instrument,
        'quantity': quantity,
        'effective_on': str(effective_on),
        'signer': signer,
        'note': note,
    })


def make_snapshot(receipts, out_of_window=None, halls=None, instruments=None):
    """按输入顺序分配自增 id。

    receipts 是窗口内凭据；out_of_window 是被窗口内冲销引用、但自身不在窗口的原签领。
    """
    numbered = []
    for index, row in enumerate(receipts, start=1):
        row = dict(row)
        row['id'] = index
        numbered.append(row)
    extra = []
    next_id = len(numbered) + 1
    for row in out_of_window or []:
        row = dict(row)
        row['id'] = next_id
        next_id += 1
        extra.append(row)
    referenced = {row['original_id'] for row in numbered if row['original_id'] is not None}
    originals = [dict(row) for row in numbered + extra if row['id'] in referenced]
    return {
        'halls': halls or [
            {'code': 'CT', 'name': '吹桐总馆', 'manager': '甲', 'purpose': ''},
            {'code': 'YH', 'name': '雁回分馆', 'manager': '乙', 'purpose': ''},
            {'code': 'ZS', 'name': '竹水分馆', 'manager': '丙', 'purpose': ''},
        ],
        'instruments': instruments or [
            {'code': 'SHENG', 'name': '笙', 'unit': '支'},
            {'code': 'DRUM', 'name': '鼓', 'unit': '面'},
            {'code': 'GONG', 'name': '锣', 'unit': '面'},
        ],
        'receipts': numbered,
        'originals': originals,
    }


def demo_snapshot():
    """贴近 README 剧本的 15 份窗口凭据，另含 1 份窗口外原签领供跨期冲销核对。"""
    october_loan = make_receipt('CT-1028-01', ReceiptKind.LOAN, 'CT', 'ZS', 'DRUM', 4, '2026-10-28')
    rows = [
        make_receipt('CT-1102-01', ReceiptKind.LOAN, 'CT', 'YH', 'SHENG', 8, '2026-11-02'),
        make_receipt('CT-1103-02', ReceiptKind.LOAN, 'CT', 'YH', 'SHENG', 5, '2026-11-03'),
        make_receipt('YH-1104-01', ReceiptKind.LOAN, 'YH', 'CT', 'SHENG', 3, '2026-11-04'),
        make_receipt('YH-1105-01', ReceiptKind.LOAN, 'YH', 'ZS', 'DRUM', 6, '2026-11-05'),
        make_receipt('ZS-1106-01', ReceiptKind.LOAN, 'ZS', 'YH', 'DRUM', 2, '2026-11-06'),
        make_receipt('ZS-1107-01', ReceiptKind.LOAN, 'ZS', 'CT', 'GONG', 4, '2026-11-07'),
        make_receipt('CT-1108-01', ReceiptKind.LOAN, 'CT', 'ZS', 'GONG', 1, '2026-11-08'),
        make_receipt('CT-1109-01', ReceiptKind.LOAN, 'CT', 'YH', 'GONG', 2, '2026-11-09'),
        make_receipt('YH-1110-01', ReceiptKind.LOAN, 'YH', 'ZS', 'SHENG', 3, '2026-11-10'),
        make_receipt('CX-1111-01', ReceiptKind.REVERSAL, 'CT', 'YH', 'SHENG', 5, '2026-11-11', original_id=2),
        make_receipt('ZS-1112-01', ReceiptKind.LOAN, 'ZS', 'CT', 'DRUM', 2, '2026-11-12'),
        make_receipt('YH-1113-01', ReceiptKind.LOAN, 'YH', 'CT', 'GONG', 2, '2026-11-13'),
        make_receipt('CT-1115-91', ReceiptKind.LOAN, 'CT', 'YH', 'SHENG', 6, '2026-11-15'),
        make_receipt('CX-1115-91', ReceiptKind.REVERSAL, 'CT', 'YH', 'SHENG', 6, '2026-11-15', original_id=13),
        # 跨期冲销：原签领是 10 月的 CT-1028-01，在快照 originals 中单独验签
        make_receipt('CX-1101-01', ReceiptKind.REVERSAL, 'CT', 'ZS', 'DRUM', 4,
                     '2026-11-01', original_id=16),
    ]
    return make_snapshot(rows, out_of_window=[october_loan])


def tamper(row, quantity=None):
    row = dict(row)
    if quantity is not None:
        row['quantity'] = quantity
    return row
