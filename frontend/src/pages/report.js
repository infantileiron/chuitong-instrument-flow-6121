import { badge, lead, section, table } from '../components/common/ui.js';
import { runNames } from '../constants/domain.js';
import { session, request } from '../stores/session.js';
import { workspace } from '../stores/workspace.js';
import { escape as e, option, period, signedNumber } from '../utils/html.js';
import { saveFile } from '../utils/download.js';

function percentCell(value, maximum) {
  const tone = value > 0 ? 'flow-positive' : value < 0 ? 'flow-negative' : 'flow-zero';
  const fill = maximum ? Math.round(Math.abs(value) / maximum * 70) + 15 : 0;
  return `<td class="matrix-value ${tone}" style="--fill:${fill}%"><strong>${value === 0 ? '·' : signedNumber(value).replace(/^\+/, '')}</strong></td>`;
}

function matrix(scope, halls) {
  const maximum = Math.max(0, ...Object.values(scope.matrix).flatMap(Object.values).map(Math.abs));
  const grid = halls.map(source => `<tr><th scope="row">${e(source.name)}</th>${halls.map(destination =>
    source.code === destination.code ? '<td class="diagonal">—</td>' : percentCell(scope.matrix[source.code][destination.code], maximum)).join('')}
    <td class="margin-num">${signedNumber(scope.halls[source.code].out).replace(/^\+/, '')}</td></tr>`).join('');
  const footer = `<tr class="matrix-footer"><th scope="row">入借合计</th>${halls.map(hall =>
    `<td class="margin-num">${signedNumber(scope.halls[hall.code].in).replace(/^\+/, '')}</td>`).join('')}
    <td class="margin-num">${signedNumber(scope.total_in).replace(/^\+/, '')}</td></tr>`;
  return `<div class="matrix">${table(['出借 ↓ / 入借 →', ...halls.map(hall => hall.name), '出借合计'], grid + footer, '')}</div>`;
}

function pairs(report, familyCode) {
  const { result, catalog } = report;
  const halls = Object.fromEntries(catalog.halls.map(hall => [hall.code, hall.name]));
  const rows = catalog.instruments.filter(item => !familyCode || item.code === familyCode).flatMap(item =>
    result.family[item.code].pairs.map(pair => {
      const destination = pair.balance > 0 ? halls[pair.right] : halls[pair.left];
      return `<tr><th scope="row">${e(item.name)}</th><td>${e(halls[pair.left])} ↔ ${e(halls[pair.right])}</td>
      <td class="number">${pair.forward}</td><td class="number">${pair.backward}</td><td class="number">${pair.matched} ${e(item.unit)}</td>
      <td>${pair.balance ? `偏向 ${e(destination)} ${Math.abs(pair.balance)} ${e(item.unit)}` : '两向持平'}</td></tr>`;
    })).join('');
  return section('馆对相抵明细', '同器材族按两向有效量相抵，负向冲销保留在有符号方向中。',
    table(['器材族', '馆对', '左 → 右', '右 → 左', '双向相抵', '净方向'], rows, '该周期没有需要核对的方向流量。'));
}

function badgeTone(status) {
  return status === 'done' ? 'green' : status === 'failed' ? 'red' : '';
}

function progressPanel(report, canResume) {
  const { progress, batch_rows: batchRows } = report;
  if (!progress || report.status === 'done') return '';
  const chips = batchRows.map(batch =>
    `<span class="batch-chip ${batch.status}" title="批次 ${batch.batch_no}">第 ${batch.batch_no} 批 · ${e(runNames[batch.status] || batch.status)}</span>`).join('');
  const resumeButton = canResume && report.status !== 'failed'
    ? '<button id="resume" class="outline">中断接手 ↻</button>' : '';
  return `<section class="progress-card" role="status" aria-live="polite">
    <div class="progress-head"><strong>${progress.percent}%</strong>
      <span>已核算 ${progress.receipts_done} / ${progress.receipt_total} 份凭据 ·
      批次 ${progress.done_batches} / ${progress.total_batches} 完成
      （每批 ${report.batch_size} 份，沿用本次冻结快照）</span></div>
    <div class="progress-track"><div class="progress-fill" style="width:${progress.percent}%"></div></div>
    <div class="batch-grid">${chips}</div>
    <div class="progress-actions">${resumeButton}<p class="error" id="resume-error" role="alert"></p></div>
    ${report.status === 'failed' ? `<p class="error" role="alert">${e(report.error || '核验失败')}</p>` :
      '<p class="inline-note">全部批次核验完成前不会生成净流报告与 CSV。</p>'}</section>`;
}

function reportBody(report, familyCode = '') {
  const { result, catalog } = report;
  const scope = familyCode ? result.family[familyCode] : result;
  const family = catalog.instruments.find(item => item.code === familyCode);
  const netRows = catalog.halls.map(hall => {
    const total = scope.halls[hall.code];
    return `<tr><th scope="row">${e(hall.name)}</th><td>${total.in}</td><td>${total.out}</td>
      <td class="number ${total.net < 0 ? 'negative' : 'positive'}">${signedNumber(total.net)}</td></tr>`;
  }).join('');
  return `<div class="report-grid">${section('馆际流向矩阵', family ? `${family.name} · 单位 ${family.unit}` : '全部器材 · 按件数汇总',
      matrix(scope, catalog.halls) + '<div class="matrix-legend"><span></span>有效调借　 <span></span>跨期冲销净减</div>',
      `<label class="filter-label">器材族<select id="family">${option('', '全部器材', familyCode)}${catalog.instruments.map(item => option(item.code, item.name, familyCode)).join('')}</select></label>`)}
    ${section('馆点净流', '净流 = 入借 − 出借', table(['馆点', '入借', '出借', '净流'], netRows, '') +
      `<div class="net-foot"><span>守恒差额</span><strong>${scope.net_total}</strong></div>`)}</div>${pairs(report, familyCode)}`;
}

export function reportPage() {
  const report = workspace.report;
  if (!report) return lead('净流报告', '核对跨馆调借净额，查看或导出计算快照。') +
    '<div class="empty">没有可阅报告。分析员可从透视方案提交计算，业务复核席发布后可供观察席查看。</div>';
  const { result } = report;
  const picker = `<label class="filter-label">选择报告<select id="report-picker">${workspace.runs.map(row =>
    option(row.id, `${row.series} v${row.version} · #${row.id}`, report.id)).join('')}</select></label>`;
  const heading = lead('净流报告', '以签名凭据核对跨馆调借流向与收支平衡。', picker);
  const summary = `<section class="report-banner"><div><span class="banner-date code">${period(report.start_on, report.end_on)}</span>
    <h2>${e(report.series)} <span class="version">v${report.version}</span></h2>
    <div class="banner-badges">${badge(runNames[report.status], badgeTone(report.status))}
    ${badge(report.published_at ? '已发布' : '待复核')}</div></div>
    <div class="report-actions">${result && session.user.role === 'reviewer' && !report.published_at ? '<button id="publish" class="primary">复核通过并发布</button>' : ''}
    ${result ? '<button id="export" class="outline">下载 CSV ↓</button>' : ''}<p class="error" id="report-error" role="alert"></p></div></section>`;
  if (!result) return heading + summary + progressPanel(report, session.user.role === 'analyst') +
    `<div class="empty" ${report.status === 'queued' || report.status === 'running' ? 'role="status"' : ''}>
    ${report.status === 'failed' ? e(report.error || '核验失败，请核对凭据后重新运行。')
      : '后台正在分批核算这一批签名凭据，可在此查看进度；全部批次完成后才显示行列核对与报告。'}</div>`;
  const stats = `<dl class="stats"><div><dt>有效调借</dt><dd>${result.total_out}<small> 件</small></dd></div>
    <div><dt>签名冲销量</dt><dd>${result.reversal}<small> 件</small></dd></div>
    <div><dt>双向相抵</dt><dd>${result.matched}<small> 件</small></dd></div>
    <div><dt>全馆净流合计</dt><dd>${result.net_total}<small> 件</small></dd></div></dl>`;
  const lineage = section('核验凭据', '快照冻结在提交运行时，后续补录由新运行纳入。', `<div class="lineage">
    <p>${result.receipt_count} 份生效凭据 · 签领 ${result.loan} − 冲销 ${result.reversal} = 有效调借 ${result.total_out}</p>
    <p>快照 SHA-256 <span class="code">${e(report.snapshot_hash)}</span></p>
    <details><summary>展开凭据编号</summary><p class="code">${report.receipt_refs.map(e).join('　') || '无'}</p></details>
    ${report.published_at ? `<p>复核席位 <span class="code">${e(report.published_by)}</span>　发布时间（UTC） <span class="code">${e(report.published_at.replace('T', ' '))}</span></p>` : ''}</div>`);
  return heading + summary + stats + `<div id="report-body">${reportBody(report)}</div>` + lineage;
}

export function bindReport(refresh) {
  const report = workspace.report;
  document.querySelector('#report-picker')?.addEventListener('change', event => { location.hash = `report/${event.target.value}`; });
  const swapFamily = () => document.querySelector('#family')?.addEventListener('change', event => {
    const selected = event.target.value;
    document.querySelector('#report-body').innerHTML = reportBody(report, selected);
    swapFamily();
    document.querySelector('#family').focus();
  });
  swapFamily();
  const action = (selector, handler, errorSelector = '#report-error') =>
    document.querySelector(selector)?.addEventListener('click', async event => {
      const button = event.currentTarget;
      button.disabled = true;
      const errorBox = document.querySelector(errorSelector);
      if (errorBox) errorBox.textContent = '';
      try { await handler(); }
      catch (error) { if (errorBox) errorBox.textContent = error.message; }
      finally { button.disabled = false; }
    });
  action('#publish', async () => {
    await request(`/runs/${report.id}/publish`, { body: {} });
    await refresh('报告已发布，馆务观察席现可查阅。');
  });
  action('#export', async () => {
    const blob = await request(`/runs/${report.id}/export`, { raw: true });
    saveFile(blob, `flow-${report.id}.csv`);
  });
  action('#resume', async () => {
    await request(`/runs/${report.id}/resume`, { body: {} });
    await refresh('已接手该运行，从未完成批次继续核算，沿用本次冻结的凭据快照。');
  }, '#resume-error');
  if (report.status === 'queued' || report.status === 'running') return setTimeout(() => refresh(), 1200);
}
