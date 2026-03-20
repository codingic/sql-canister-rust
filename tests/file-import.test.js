import assert from 'node:assert/strict';
import { read, utils, write } from 'xlsx';
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