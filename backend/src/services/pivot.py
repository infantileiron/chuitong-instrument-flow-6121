from itertools import combinations, permutations

from src.constants.domain import ReceiptKind
from src.utils.crypto import signed


class LedgerMismatch(ValueError):
    pass


ROUTE_FIELDS = ('source', 'destination', 'instrument', 'quantity')


def split_batches(receipts, batch_size):
    """按冻结快照的凭据序号固定切批，边界只依赖快照本身，续跑时不漂移。"""
    if batch_size < 1:
        raise ValueError('batch_size 必须为正整数')
    return [(start, min(start + batch_size, len(receipts)))
            for start in range(0, len(receipts), batch_size)]


def _empty_accumulator():
    return {}


def _add(accumulator, row, is_reversal):
    direction = accumulator.setdefault(row['instrument'], {}).setdefault(
        row['source'], {}).setdefault(row['destination'], [0, 0])
    direction[int(is_reversal)] += row['quantity']


def _merge_accumulator(target, partial):
    for instrument, sources in partial.items():
        for source, destinations in sources.items():
            for destination, counts in destinations.items():
                slot = target.setdefault(instrument, {}).setdefault(source, {}).setdefault(destination, [0, 0])
                slot[0] += counts[0]
                slot[1] += counts[1]


def _as_events(accumulator):
    """嵌套累加器还原为旧口径的 (族, 出借馆, 入借馆) -> [签领, 冲销] 映射。"""
    events = {}
    for instrument, sources in accumulator.items():
        for source, destinations in sources.items():
            for destination, counts in destinations.items():
                events[(instrument, source, destination)] = counts
    return events


def process_batch(snapshot, start_index, end_index, already_reversed):
    """核算一个批次：验签、冲销跨批配对、同族同方向件数累加。

    already_reversed 是此前已完成批次登记过的原签领 id 集合；
    同一张原单在本批或跨批被冲销第二次都会在此抛出 LedgerMismatch。
    返回 (partial, reversed_ids)，两者均可直接 JSON 持久化。
    """
    originals = {row['id']: row for row in snapshot['originals']}
    accumulator = _empty_accumulator()
    reversed_here = set()
    seen = set(already_reversed)
    for row in snapshot['receipts'][start_index:end_index]:
        if not signed(row):
            raise LedgerMismatch(f'{row["reference"]} 的签名校验失败')
        is_reversal = row['kind'] == ReceiptKind.REVERSAL
        if is_reversal:
            original = originals.get(row['original_id'])
            if (not original or original['kind'] != ReceiptKind.LOAN or not signed(original)
                    or original['effective_on'] > row['effective_on']
                    or any(original[field] != row[field] for field in ROUTE_FIELDS)):
                raise LedgerMismatch(f'{row["reference"]} 与原签领无法配对')
            if row['original_id'] in seen:
                raise LedgerMismatch(f'{row["reference"]} 重复冲销原签领 {original["reference"]}')
            seen.add(row['original_id'])
            reversed_here.add(row['original_id'])
        _add(accumulator, row, is_reversal)
    return accumulator, sorted(reversed_here)


def merge_batches(partials, reversed_id_lists):
    """合并各批次的同族同方向件数与已冲销原单，最后再做一次全局双冲销防护。"""
    accumulator = _empty_accumulator()
    all_reversed = []
    for partial, reversed_ids in zip(partials, reversed_id_lists):
        _merge_accumulator(accumulator, partial)
        all_reversed.extend(reversed_ids)
    if len(set(all_reversed)) != len(all_reversed):
        raise LedgerMismatch('同一张原签领被冲销两次')
    return _as_events(accumulator), set(all_reversed)


def finalize(snapshot, signed_events, reversed_count):
    """全部批次完成后的统一口径：矩阵、双向相抵与边际守恒校验。"""
    halls = [item['code'] for item in snapshot['halls']]
    families = {}
    aggregate = matrix(halls)
    for item in snapshot['instruments']:
        family = item['code']
        directional = matrix(halls)
        directions = []
        for source, destination in permutations(halls, 2):
            loan, reversal = signed_events.get((family, source, destination), [0, 0])
            effective = loan - reversal
            directional[source][destination] = effective
            aggregate[source][destination] += effective
            directions.append({'source': source, 'destination': destination, 'loan': loan,
                               'reversal': reversal, 'effective': effective})
        pairs = []
        for left, right in combinations(halls, 2):
            forward, backward = directional[left][right], directional[right][left]
            if forward or backward:
                pairs.append({'left': left, 'right': right, 'forward': forward, 'backward': backward,
                              'matched': max(0, min(forward, backward)), 'balance': forward - backward})
        families[family] = {**margins(halls, directional), 'pairs': pairs, 'directions': directions}

    result = margins(halls, aggregate)
    result['family'] = families
    result['matched'] = sum(pair['matched'] for family in families.values() for pair in family['pairs'])
    result['loan'] = sum(counts[0] for counts in signed_events.values())
    result['reversal'] = sum(counts[1] for counts in signed_events.values())
    result['receipt_count'] = len(snapshot['receipts'])
    result['verified'] = (result['net_total'] == 0 and result['total_in'] == result['total_out']
                          == result['loan'] - result['reversal'])
    if reversed_count and len(reversed_count) != sum(
            1 for row in snapshot['receipts'] if row['kind'] == ReceiptKind.REVERSAL):
        raise LedgerMismatch('冲销批次登记数量与快照不一致')
    if not result['verified']:
        raise LedgerMismatch('签名净额与矩阵边际不守恒')
    return result


def calculate(snapshot, batch_size=None):
    """一次算完的参考实现，内部走与 worker 相同的分批原语，保证两条口径恒等。"""
    size = batch_size or len(snapshot['receipts']) or 1
    partials, reversed_lists = [], []
    seen = set()
    for start, end in split_batches(snapshot['receipts'], size):
        partial, reversed_ids = process_batch(snapshot, start, end, seen)
        partials.append(partial)
        reversed_lists.append(reversed_ids)
        seen.update(reversed_ids)
    events, reversed_set = merge_batches(partials, reversed_lists)
    return finalize(snapshot, events, reversed_set)


def matrix(halls):
    return {source: dict.fromkeys(halls, 0) for source in halls}


def margins(halls, directional):
    outgoing = {source: sum(directional[source].values()) for source in halls}
    incoming = {destination: sum(directional[source][destination] for source in halls) for destination in halls}
    hall_totals = {hall: {'in': incoming[hall], 'out': outgoing[hall], 'net': incoming[hall] - outgoing[hall]}
                   for hall in halls}
    return {'matrix': directional, 'halls': hall_totals, 'total_in': sum(incoming.values()),
            'total_out': sum(outgoing.values()), 'net_total': sum(row['net'] for row in hall_totals.values())}
