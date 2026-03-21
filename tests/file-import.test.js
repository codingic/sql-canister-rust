import assert from 'node:assert/strict';
import { read, utils, write } from 'xlsx';
import { queryResultToSqlText, queryResultToTsvText, queryResultToXlsxBytes } from '../frontend/exporter.js';
import { workbookToSqlText } from '../frontend/importer.js';

function runCase(name, fn) {
  fn();
  console.log(`PASS ${name}`);
}

function createWorkbookBuffer() {
  const workbook = utils.book_new();
  const worksheet = utils.aoa_to_sheet([
    ['编号', '姓名', '城市', '积分'],
    [1, '张三', '深圳', 88.5],
    [2, '李四', '杭州', 91],
  ]);

  utils.book_append_sheet(workbook, worksheet, '供应商数据');
  return write(workbook, { type: 'buffer', bookType: 'xlsx' });
}

runCase('xlsx workbook converts to sql text', () => {
  const workbook = read(createWorkbookBuffer(), { type: 'buffer' });
  const sqlText = workbookToSqlText(workbook, '供应商.xlsx');

  assert.match(sqlText, /^BEGIN;/);
  assert.match(sqlText, /CREATE TABLE "供应商数据"/);
  assert.match(sqlText, /"编号" INTEGER/);
  assert.match(sqlText, /"姓名" TEXT/);
  assert.match(sqlText, /"积分" REAL/);
  assert.match(sqlText, /INSERT INTO "供应商数据"/);
  assert.match(sqlText, /VALUES \(1, '张三', '深圳', 88\.5\), \(2, '李四', '杭州', 91\)/);
  assert.match(sqlText, /'张三'/);
  assert.match(sqlText, /'李四'/);
  assert.match(sqlText, /88\.5/);
  assert.match(sqlText, /COMMIT;$/);
});

runCase('query result exports to sql text', () => {
  const sqlText = queryResultToSqlText(
    '供应商数据',
    ['编号', '姓名', '积分', '备注'],
    [
      [{ Integer: 1 }, { Text: '张三' }, { Float: 88.5 }, { Null: null }],
      [{ Integer: 2 }, { Text: '李四' }, { Integer: 91 }, { Text: 'VIP' }],
    ]
  );

  assert.match(sqlText, /^BEGIN;/);
  assert.match(sqlText, /CREATE TABLE "供应商数据"/);
  assert.match(sqlText, /"编号" INTEGER/);
  assert.match(sqlText, /"姓名" TEXT/);
  assert.match(sqlText, /"积分" REAL/);
  assert.match(sqlText, /"备注" TEXT/);
  assert.match(sqlText, /INSERT INTO "供应商数据"/);
  assert.match(sqlText, /VALUES \(1, '张三', 88\.5, NULL\), \(2, '李四', 91, 'VIP'\)/);
  assert.match(sqlText, /COMMIT;$/);
});

runCase('query result exports to xlsx bytes', () => {
  const xlsxBytes = queryResultToXlsxBytes(
    '供应商数据',
    ['编号', '姓名', '积分', '备注'],
    [
      [{ Integer: 1 }, { Text: '张三' }, { Float: 88.5 }, { Null: null }],
      [{ Integer: 2 }, { Text: '李四' }, { Integer: 91 }, { Text: 'VIP' }],
    ]
  );
  const workbook = read(Buffer.from(xlsxBytes), { type: 'buffer' });
  const rows = utils.sheet_to_json(workbook.Sheets['供应商数据'], {
    header: 1,
    raw: true,
    defval: null,
  });

  assert.deepEqual(rows, [
    ['编号', '姓名', '积分', '备注'],
    [1, '张三', 88.5, null],
    [2, '李四', 91, 'VIP'],
  ]);
});

runCase('query result exports to tsv text', () => {
  const tsvText = queryResultToTsvText(
    ['编号', '姓名', '备注'],
    [
      [{ Integer: 1 }, { Text: '张三' }, { Text: '普通客户' }],
      [{ Integer: 2 }, { Text: '李四' }, { Text: '包含\n换行' }],
    ]
  );

  assert.equal(
    tsvText,
    ['编号\t姓名\t备注', '1\t张三\t普通客户', '2\t李四\t"包含\n换行"'].join('\n')
  );
});