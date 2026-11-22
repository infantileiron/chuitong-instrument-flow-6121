"""pivot 分批核算原语测试：恒等性、跨批双冲销、跨期冲销、伪造签名。"""
import copy

from tests import harness  # noqa: F401  安装 stub 后再导入业务模块
from src.constants.domain import ReceiptKind
from src.services.pivot import (LedgerMismatch, calculate, finalize, merge_batches,
                                process_batch, split_batches)
from src.utils.crypto import canonical

from tests.harness import demo_snapshot, make_receipt, make_snapshot, tamper


def test_split_batches_boundaries():
    assert split_batches(list(range(10)), 3) == [(0, 3), (3, 6), (6, 9), (9, 10)]
    assert split_batches([], 5) == []
    assert split_batches(list(range(4)), 5) == [(0, 4)]
    assert split_batches(list(range(5)), 1) == [(i, i + 1) for i in range(5)]


def test_batch_equals_one_shot():
    snapshot = demo_snapshot()
    one_shot = calculate(snapshot)  # 不分批
    for size in (1, 2, 3, 4, 5, 7, 100):
        batched = calculate(snapshot, batch_size=size)
        assert canonical(batched) == canonical(one_shot), f'批次大小 {size} 与一次算完不一致'
    # README 剧本口径：15 份凭据、签领 44、冲销 15、有效调借 29
    assert one_shot['receipt_count'] == 15
    assert one_shot['loan'] == 44
    assert one_shot['reversal'] == 15
    assert one_shot['total_out'] == 29
    assert one_shot['net_total'] == 0
    assert one_shot['verified'] is True
    # 三馆净流 +4 / -2 / -2
    nets = {hall: one_shot['halls'][hall]['net'] for hall in ('CT', 'YH', 'ZS')}
    assert nets == {'CT': 4, 'YH': -2, 'ZS': -2}


def test_manual_merge_matches_finalize():
    snapshot = demo_snapshot()
    partials, reversed_lists, seen = [], [], set()
    for start, end in split_batches(snapshot['receipts'], 2):
        partial, reversed_ids = process_batch(snapshot, start, end, seen)
        partials.append(partial)
        reversed_lists.append(reversed_ids)
        seen.update(reversed_ids)
    events, reversed_set = merge_batches(partials, reversed_lists)
    manual = finalize(snapshot, events, reversed_set)
    assert canonical(manual) == canonical(calculate(snapshot))


def test_cross_period_reversal_pairs_with_outside_original():
    # CX-1101-01 冲销 10 月原签领，窗口内出现负向量但仍守恒
    result = calculate(demo_snapshot(), batch_size=3)
    drum = result['family']['DRUM']
    # CT -> ZS：YH-1105 不经 CT->ZS；CT->ZS 签领为 0，冲销 4 => -4；ZS->CT 签领 2
    assert drum['matrix']['CT']['ZS'] == -4
    assert drum['matrix']['ZS']['CT'] == 2
    pair = next(p for p in drum['pairs'] if {p['left'], p['right']} == {'CT', 'ZS'})
    assert pair['matched'] == 0
    assert pair['balance'] == -6


def test_double_reversal_same_batch_errors():
    loan = make_receipt('L-1', ReceiptKind.LOAN, 'CT', 'YH', 'SHENG', 3, '2026-11-02')
    r1 = make_receipt('CX-1', ReceiptKind.REVERSAL, 'CT', 'YH', 'SHENG', 3, '2026-11-03', original_id=1)
    r2 = make_receipt('CX-2', ReceiptKind.REVERSAL, 'CT', 'YH', 'SHENG', 3, '2026-11-04', original_id=1)
    snapshot = make_snapshot([loan, r1, r2])
    try:
        calculate(snapshot, batch_size=10)
    except LedgerMismatch as exc:
        assert '重复冲销' in str(exc)
    else:
        raise AssertionError('同批双冲销应当报错')


def test_double_reversal_across_batches_errors():
    loan = make_receipt('L-1', ReceiptKind.LOAN, 'CT', 'YH', 'SHENG', 3, '2026-11-02')
    r1 = make_receipt('CX-1', ReceiptKind.REVERSAL, 'CT', 'YH', 'SHENG', 3, '2026-11-03', original_id=1)
    r2 = make_receipt('CX-2', ReceiptKind.REVERSAL, 'CT', 'YH', 'SHENG', 3, '2026-11-04', original_id=1)
    snapshot = make_snapshot([loan, r1, r2])
    # 第一批只处理 loan+cx1，第二批处理 cx2；续跑时 seen 已含原单 id
    partial1, rev1 = process_batch(snapshot, 0, 2, set())
    assert rev1 == [1]
    try:
        process_batch(snapshot, 2, 3, set(rev1))
    except LedgerMismatch as exc:
        assert '重复冲销' in str(exc)
    else:
        raise AssertionError('跨批双冲销应当报错')


def test_merged_guard_also_rejects_duplicate():
    # 即使绕过逐批检查，merge_batches 还有一道全局去重
    try:
        merge_batches([{}, {}], [[7], [7]])
    except LedgerMismatch:
        pass
    else:
        raise AssertionError('合并阶段的全局双冲销防护未生效')


def test_forged_signature_fails():
    snapshot = demo_snapshot()
    forged = copy.deepcopy(snapshot)
    forged['receipts'][0]['quantity'] = 99  # 改数量但不重签
    try:
        calculate(forged, batch_size=4)
    except LedgerMismatch as exc:
        assert '签名校验失败' in str(exc)
    else:
        raise AssertionError('伪造签名应当使运行失败')


def test_forged_original_signature_fails():
    snapshot = demo_snapshot()
    forged = copy.deepcopy(snapshot)
    original = next(row for row in forged['originals'] if row['reference'] == 'CT-1028-01')
    original['quantity'] = 99  # 窗口外原签领被篡改
    try:
        calculate(forged, batch_size=4)
    except LedgerMismatch as exc:
        assert '无法配对' in str(exc)
    else:
        raise AssertionError('原签领签名失效应当报错')


def test_reversal_route_mismatch_fails():
    loan = make_receipt('L-1', ReceiptKind.LOAN, 'CT', 'YH', 'SHENG', 3, '2026-11-02')
    bad = make_receipt('CX-1', ReceiptKind.REVERSAL, 'YH', 'CT', 'SHENG', 3, '2026-11-03', original_id=1)
    snapshot = make_snapshot([loan, bad])
    try:
        calculate(snapshot)
    except LedgerMismatch as exc:
        assert '无法配对' in str(exc)
    else:
        raise AssertionError('方向不一致的冲销应当配对失败')


def test_partials_are_json_safe_and_additive():
    snapshot = demo_snapshot()
    partial, reversed_ids = process_batch(snapshot, 0, 5, set())
    restored = canonical(partial)  # 不抛异常即可 JSON 持久化
    assert isinstance(restored, str) and reversed_ids == sorted(reversed_ids)
    # 同方向件数可加：两批分别算后合并 == 合起来算
    p1, r1 = process_batch(snapshot, 0, 7, set())
    p2, r2 = process_batch(snapshot, 7, 15, set(r1))
    events, _ = merge_batches([p1, p2], [r1, r2])
    whole_partial, whole_rev = process_batch(snapshot, 0, 15, set())
    whole_events, _ = merge_batches([whole_partial], [whole_rev])
    assert events == whole_events


def test_margin_conservation_holds_for_all_splits():
    snapshot = demo_snapshot()
    for size in range(1, 16):
        result = calculate(snapshot, batch_size=size)
        assert result['total_in'] == result['total_out'] == result['loan'] - result['reversal']
        assert result['net_total'] == 0


def test_empty_snapshot_still_verifies():
    snapshot = make_snapshot([])
    assert split_batches(snapshot['receipts'], 5) == []
    result = calculate(snapshot, batch_size=5)
    assert result['receipt_count'] == 0 and result['loan'] == 0 and result['reversal'] == 0
    assert result['verified'] is True and result['net_total'] == 0
    assert result['matched'] == 0 and result['total_out'] == 0
