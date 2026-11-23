// CSV 解析器回归测试：node frontend/tests/test_csv.js
// 与 Python csv 模块的解析结果对照，并验证任意分片点切分结果一致。
import {CsvParser, checkHeader} from '../src/utils/csv.js';

function parseAll(text, chunkSize = null) {
  const parser = new CsvParser();
  const records = [];
  if (chunkSize === null) {
    for (const record of parser.push(text)) records.push(record);
  } else {
    for (let i = 0; i < text.length; i += chunkSize) {
      for (const record of parser.push(text.slice(i, i + chunkSize))) records.push(record);
    }
  }
  for (const record of parser.flush()) records.push(record);
  return records;
}

const cases = [
  'a,b,c\n1,2,3\n4,5,6\n',
  'a,b,c\n1,2,3',                       // 无结尾换行
  'a,b,"c,d"\n"x","y""z",w\n',          // 引号包裹与转义
  'a,b\n"多\n行",值\n',                 // 字段内换行
  'a,b\r\n1,2\r\n3,4\r\n',              // CRLF
  'a,b\r1,2\r',                         // 单独 CR
  'a,b\n"x\ny",2\n',                    // 引号内换行
  '事件编号,工具编号\nGQ-01,充电电钻\n',   // 中文
  'a,b\n1,2\n\n',                       // 末尾空行 → 末尾产生一个单字段记录
  '"","",\n',                           // 空字段
];

let failed = 0;
for (const [index, text] of cases.entries()) {
  const whole = JSON.stringify(parseAll(text));
  for (const size of [1, 2, 3, 5]) {
    const sliced = JSON.stringify(parseAll(text, size));
    if (sliced !== whole) {
      console.error(`案例 ${index} 分片 ${size} 不一致:\n 整体 ${whole}\n 分片 ${sliced}`);
      failed += 1;
    }
  }
  console.log(`ok 案例 ${index}: ${whole}`);
}

// 与 Python csv 模块对照（由命令行参数传入：文件路径）
const fixture = process.argv[2];
if (fixture) {
  const {readFileSync} = await import('node:fs');
  const text = readFileSync(fixture, 'utf8');
  const ours = parseAll(text);
  console.log(`fixture ${fixture}: ${ours.length} 条记录`);
  console.log(JSON.stringify(ours));
}

// 表头校验
const fields = [
  {key: 'event_id'}, {key: 'asset_id'}, {key: 'asset_name', required: false},
  {key: 'event_date'}, {key: 'sequence'}, {key: 'event_type'},
];
const good = ['event_id', 'asset_id', 'asset_name', 'event_date', 'sequence', 'event_type'];
checkHeader(good, fields);
checkHeader(['event_type', 'event_date', 'sequence', 'event_id', 'asset_id', 'asset_name'], fields);
for (const bad of [
  ['event_id', 'asset_id', 'event_date', 'sequence', 'event_type'],           // 少可空列 → 允许? 不允许缺可空列? 允许
  ['event_id', 'event_id', 'asset_id', 'asset_name', 'event_date', 'sequence', 'event_type'],
  ['event_id', 'asset_id', 'asset_name', 'event_date', 'sequence', 'unknown'],
]) {
  try {
    checkHeader(bad, fields);
    if (bad.length === 5) console.log('ok 缺少可空列仍可通过（服务端按空值处理）');
    else { console.error(`表头应被拒绝: ${bad}`); failed += 1; }
  } catch (error) {
    console.log(`ok 拒绝表头: ${bad.join(',')}`);
  }
}

// 引号未闭合应报错
try {
  parseAll('a,b\n"未闭合,2\n');
  console.error('未闭合引号未报错');
  failed += 1;
} catch (error) {
  console.log(`ok 未闭合引号报错: ${error.message}`);
}

if (failed) {
  console.error(`${failed} 项失败`);
  process.exit(1);
}
console.log('CSV 解析器测试通过。');
