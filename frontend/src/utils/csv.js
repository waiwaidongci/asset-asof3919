// 流式 CSV 解析：分片喂入文本，逐条产出记录，内存不随文件大小增长。
// 支持引号包裹、双引号转义、字段内换行与 CRLF；格式错误时抛出带行号的异常。
export class CsvParser {
  constructor() {
    this.field = '';
    this.record = [];
    this.state = 'outside'; // outside | inQuotes | afterQuote | inUnquoted
    this.skipLf = false;
    this.line = 1;
  }

  endField() {
    this.record.push(this.field);
    this.field = '';
    this.state = 'outside';
  }

  endRecord() {
    this.endField();
    const record = this.record;
    this.record = [];
    this.line += 1;
    return record;
  }

  *push(text) {
    for (const ch of text) {
      if (this.skipLf) {
        this.skipLf = false;
        if (ch === '\n') continue;
      }
      if (this.state === 'inQuotes') {
        if (ch === '"') this.state = 'afterQuote';
        else this.field += ch;
        continue;
      }
      if (this.state === 'afterQuote') {
        if (ch === '"') {
          this.field += '"';
          this.state = 'inQuotes';
          continue;
        }
        if (ch === ',') {
          this.endField();
          continue;
        }
        if (ch === '\r' || ch === '\n') {
          if (ch === '\r') this.skipLf = true;
          yield this.endRecord();
          continue;
        }
        throw new Error(`CSV 第 ${this.line} 行附近格式不正确`);
      }
      // outside 或 inUnquoted：结构字符统一处理
      if (ch === ',') {
        this.endField();
        continue;
      }
      if (ch === '\r' || ch === '\n') {
        if (ch === '\r') this.skipLf = true;
        yield this.endRecord();
        continue;
      }
      if (this.state === 'outside') {
        if (ch === '"') {
          this.state = 'inQuotes';
          continue;
        }
        this.state = 'inUnquoted';
      }
      this.field += ch;
    }
  }

  *flush() {
    if (this.skipLf) this.skipLf = false;
    if (this.state === 'inQuotes') throw new Error(`CSV 第 ${this.line} 行附近格式不正确`);
    if (this.state === 'afterQuote' || this.state === 'inUnquoted' || this.field !== '' || this.record.length) {
      yield this.endRecord();
    }
  }
}

// 校验表头与字段说明一致，返回表头数组。
export function checkHeader(header, fields) {
  const keys = fields.map(field => field.key);
  const required = fields.filter(field => field.required !== false).map(field => field.key);
  const valid = header.length === new Set(header).size
    && required.every(key => header.includes(key))
    && header.every(key => keys.includes(key));
  if (!valid) throw new Error('CSV 表头与字段说明不符，请下载模板检查列名');
  return header;
}
