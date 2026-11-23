import {feedback, field} from '../components/common/fields.js';
import {empty, panel, pill} from '../components/common/panels.js';
import {table} from '../components/common/table.js';
import {DATASET_STATUS, STATUS} from '../constants/enums.js';
import {handler} from '../hooks/action.js';
import {session} from '../stores/session.js';
import {workspace} from '../stores/workspace.js';
import {CsvParser, checkHeader} from '../utils/csv.js';
import {button, el, link, p} from '../utils/dom.js';
import {date} from '../utils/format.js';

// 与后端 BATCH_SIZE 一致：站里机器一次最多暂放两千笔
const CHUNK = 2000;
const SLICE = 4 * 1024 * 1024;

async function streamLedger(file, spec, importId, startChunk, onProgress) {
  const parser = new CsvParser();
  const decoder = new TextDecoder();
  let header = null;
  let batch = [];
  let index = 0;
  let first = true;
  const send = async () => {
    if (!batch.length) return;
    if (index >= startChunk) {
      onProgress(await workspace.sendChunk(importId, {index, rows: batch}));
    } else if (index === startChunk - 1) {
      // 续传前重发上一批：服务端校验指纹，确认还是同一本原簿
      await workspace.sendChunk(importId, {index, rows: batch});
    }
    index += 1;
    batch = [];
  };
  const handle = async record => {
    const line = parser.line - 1;
    if (!header) {
      header = checkHeader(record, spec.fields);
      return;
    }
    if (record.length !== header.length) throw new Error(`CSV 第 ${line} 行 · 工作表的列数与表头不一致`);
    batch.push(Object.fromEntries(header.map((key, i) => [key, record[i]])));
    if (batch.length === CHUNK) await send();
  };
  let offset = 0;
  while (offset < file.size) {
    let text = decoder.decode(await file.slice(offset, offset + SLICE).arrayBuffer(),
                              {stream: offset + SLICE < file.size});
    offset += SLICE;
    if (first) {
      text = text.replace(/^\uFEFF/, '');
      first = false;
    }
    for (const record of parser.push(text)) await handle(record);
  }
  for (const record of parser.flush()) await handle(record);
  await send();
  if (index < startChunk) throw new Error('所选文件比已入库进度还短，请核对原簿后重新选择');
  return index;
}

function bigImportForm(spec, datasets, refresh) {
  const pending = datasets.filter(item => item.status === 'importing');
  const error = feedback();
  const submit = button('分批导入大簿', {type: 'submit', class: 'primary'});
  const progressNote = p('', 'subtle');
  const resume = el('label', {class: 'field', for: 'resume-id'}, el('span', {}, '续传批次'),
    el('select', {id: 'resume-id', name: 'resume_id'},
      el('option', {value: ''}, '新建导入'),
      pending.map(item => el('option', {value: item.id},
        `${item.name} · 已入库 ${item.imported_rows} 行`))));
  const form = el('form', {class: 'fields', onSubmit: handler(submit, error, async event => {
    const data = new FormData(event.target);
    const file = data.get('ledger');
    if (!file?.size) throw new Error('请选择要补盘的交接簿 CSV');
    let importId;
    let startChunk = 0;
    const resumeId = Number(data.get('resume_id')) || null;
    if (resumeId) {
      const state = await workspace.getImport(resumeId);
      if (state.status !== 'importing') throw new Error('该批次已结束入库，请刷新后重试');
      if (state.filename !== file.name) throw new Error(`所选文件与批次原簿 ${state.filename} 不符`);
      importId = state.id;
      startChunk = state.next_chunk;
    } else {
      const name = data.get('big_batch_name');
      if (!name?.trim()) throw new Error('请填写批次名称');
      importId = (await workspace.beginImport({name, filename: file.name})).id;
    }
    await streamLedger(file, spec, importId, startChunk, state => {
      progressNote.textContent = `已入库 ${state.imported_rows} 行（第 ${state.next_chunk} 批）…`;
    });
    const item = await workspace.finishImport(importId);
    const target = `#inputs/${item.id}`;
    if (location.hash === target) await refresh();
    else location.hash = target;
  })}, field({key: 'big_batch_name', label: '批次名称', type: 'string', required: false, max_length: 120}),
    resume,
    el('label', {class: 'field', for: 'ledger'}, el('span', {}, '交接簿 CSV（百万行分批入库）'),
      el('input', {id: 'ledger', type: 'file', name: 'ledger', accept: '.csv,text/csv', required: true})),
    submit, progressNote, error);
  return session.canWrite ? form : null;
}

export async function inputs(route, refresh) {
  const {spec, datasets, runs} = workspace;
  const selected = datasets.find(item => item.id === route.id) || datasets[0];
  const error = feedback();
  const sample = button('下载示例 CSV', {class:'quiet',onClick:event=>handler(sample,error,()=>workspace.download('/template',spec.sample_name))(event)});
  const detail = selected && await workspace.getDataset(selected.id, route.offset);
  const columns = spec.fields.map(field => ({key:field.key,label:field.label}));
  const statusLine = detail && detail.status !== 'ready'
    ? el('div', {class: 'empty'},
      p(detail.status === 'importing'
        ? `入库中 · 已收 ${detail.imported_rows} 行（第 ${detail.next_chunk} 批），完成后才供查阅。`
        : `入库失败：${detail.error || '请重新导入'}`))
    : null;
  const preview = detail ? panel('批次明细', el('div',{class:'actions'},
    el('label',{for:'dataset-id',class:'sr'}, '选择数据批次'),
    el('select',{id:'dataset-id','aria-label':'选择数据批次',onChange:event=>{location.hash=`#inputs/${event.target.value}`;}},
      datasets.map(item=>el('option',{value:item.id,selected:item.id === selected.id},
        item.status === 'ready' ? item.name : `${item.name}（${DATASET_STATUS[item.status]}）`)))),
    detail.status === 'ready' && link('使用此批次运行',`#run/${detail.id}`,{class:'quiet'})),
    p(`${detail.filename} · ${detail.status === 'ready' ? `${detail.row_count} 行 · 去重后 ${detail.event_count} 笔` : `已入库 ${detail.imported_rows} 行`} · ${date(detail.created_at)}`,'filename'),
    statusLine || table(columns,detail.rows,{caption:'批次明细',pageSize:16}),
    detail.status === 'ready' && el('div',{class:'pagination'},
      route.offset > 0 && link('上一批明细',`#inputs/${detail.id}/${Math.max(0,route.offset-16)}`,{class:'quiet'}),
      p(`${detail.rows.length ? route.offset+1 : route.offset}–${route.offset+detail.rows.length} / ${detail.row_count}`),
      route.offset+16 < detail.row_count && link('下一批明细',`#inputs/${detail.id}/${route.offset+16}`,{class:'quiet'})))
    : empty('没有可用的批次。请导入示例或业务 CSV。');
  const batches = selected && panel('盘点批次', table([
    {label:'运行', render:item=>link(item.name,`#results/${item.id}`)},
    {label:'状态', render:item=>pill(STATUS[item.status],item.status)},
    {label:'时间', render:item=>date(item.created_at)},
  ], runs.filter(run=>run.dataset_id===selected.id), {caption:'盘点批次', empty:'该数据批次还没有盘点运行。'}));
  const submit = button('校验并导入', {type:'submit',class:'primary'});
  const upload = session.canWrite && el('form',{class:'fields',onSubmit:handler(submit,error,async event=>{
    const data = new FormData(event.target);
    const file = data.get('source');
    if (!file?.size) throw new Error('请选择一个 CSV 文件');
    if (file.size > 2_000_000) throw new Error('CSV 最多 2 MB，更大的交接簿请用下方分批导入');
    const item = await workspace.upload({name:data.get('batch_name'),filename:file.name,csv_text:await file.text()});
    const target = `#inputs/${item.id}`;
    if (location.hash === target) await refresh();
    else location.hash = target;
  })},field({key:'batch_name',label:'批次名称',type:'string',max_length:120}),
    el('label',{class:'field',for:'source'}, el('span',{},'业务 CSV'),el('input',{id:'source',type:'file',name:'source',accept:'.csv,text/csv',required:true})), submit);
  return el('div',{class:'two-col'},el('div',{class:'wide-panel'},preview,batches),el('aside',{class:'sidebar-panel'},
    panel('数据导入',p('导入后保留原批次和校验指纹，可用同一份数据复跑。','subtle'),sample,upload,error),
    panel('大簿补盘',p('往年交接簿按每批两千笔分片入库，中断后选择续传批次从原进度续跑，重复分片不重复计数；同号同内容只算一次，同号异件会指出原行。','subtle'),
      bigImportForm(spec, datasets, refresh)),
    panel('字段说明',el('dl',{class:'dictionary'},spec.fields.map(item=>el('div',{},el('dt',{},item.label),
      el('dd',{},item.key,item.required === false ? ' · 可空' : ' · 必填',item.options ? ` · ${item.options.join(' / ')}` : '')))))));
}
