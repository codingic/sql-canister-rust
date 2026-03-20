import assert from 'node:assert/strict';
import { execFileSync } from 'node:child_process';
import { dirname, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const currentDir = dirname(fileURLToPath(import.meta.url));
const projectRoot = resolve(currentDir, '..');
let tableCounter = 0;

function runDfx(args, options = {}) {
  const output = execFileSync('dfx', args, {
    cwd: projectRoot,
    encoding: 'utf8',
    stdio: ['ignore', 'pipe', 'pipe'],
    ...options,
  });

  if (typeof output !== 'string') {
    return '';
  }

  return output.trim();
}

function resetLocalReplica() {
  try {
    execFileSync('dfx', ['stop'], {
      cwd: projectRoot,
      stdio: 'ignore',
    });
  } catch {
    // Ignore stop failures when no local replica is running.
  }

  runDfx(['start', '--clean', '--background'], {
    stdio: 'inherit',
  });
}

function unwrapOk(result, action) {
  if ('Err' in result) {
    assert.fail(`${action} failed: ${result.Err}`);
  }

  return result.Ok;
}

function unwrapErr(result, action) {
  if ('Ok' in result) {
    assert.fail(`${action} unexpectedly succeeded`);
  }

  return result.Err;
}

function createTableName(prefix) {
  tableCounter += 1;
  return `${prefix}_${Date.now().toString(36)}_${tableCounter}`;
}

function sqlLiteral(value) {
  if (value === null) {
    return 'NULL';
  }

  if (typeof value === 'string') {
    return `'${value.replaceAll("'", "''")}'`;
  }

  return String(value);
}

function createTableSql(tableName, columns) {
  return `CREATE TABLE ${tableName} (${columns
    .map(({ name, type }) => `${name} ${type}`)
    .join(', ')})`;
}

function insertSql(tableName, columns, values) {
  return `INSERT INTO ${tableName} (${columns.map(({ name }) => name).join(', ')}) VALUES (${values
    .map(sqlLiteral)
    .join(', ')})`;
}

function callCanister(method, candidArgs = []) {
  const output = runDfx([
    'canister',
    'call',
    'backend',
    method,
    ...candidArgs,
    '--output',
    'json',
  ]);

  return JSON.parse(output);
}

function encodeTextVecCandid(values) {
  return `(vec { ${values.map((value) => JSON.stringify(value)).join('; ')} })`;
}

async function executeSql(sql, action = 'execute sql') {
  return callCanister('execute', [`(${JSON.stringify(sql)})`]);
}

async function batchExecuteSql(statements, action = 'execute batch sql') {
  return callCanister('execute_batch', [encodeTextVecCandid(statements)]);
}

async function querySql(sql, action = 'query sql') {
  return callCanister('query', [`(${JSON.stringify(sql)})`]);
}

async function infoSql() {
  return callCanister('info');
}

async function assertExecuteOk(sql, action) {
  const result = unwrapOk(await executeSql(sql, action), action);
  assert.equal(result.message, 'SQL statement executed successfully');
  return result;
}

async function assertBatchExecuteOk(statements, action) {
  return unwrapOk(await batchExecuteSql(statements, action), action);
}

async function assertQueryOk(sql, action) {
  return unwrapOk(await querySql(sql, action), action);
}

async function runCase(name, fn) {
  await fn();
  console.log(`PASS ${name}`);
}

async function testFreshInfo() {
  const databaseInfo = await infoSql();
  assert.deepEqual(databaseInfo.tables, []);
}

async function testInfoSorting() {
  const laterTable = createTableName('zeta');
  const earlierTable = createTableName('alpha');

  await assertExecuteOk(
    `CREATE TABLE ${laterTable} (id INTEGER)`,
    'create later table'
  );
  await assertExecuteOk(
    `CREATE TABLE ${earlierTable} (id INTEGER)`,
    'create earlier table'
  );

  const databaseInfo = await infoSql();
  const matchingTables = databaseInfo.tables.filter(
    (table) => table === earlierTable || table === laterTable
  );

  assert.deepEqual(matchingTables, [earlierTable, laterTable]);
}

async function testCreateTablesAndInsertRows() {
  const usersTable = createTableName('users');
  const ordersTable = createTableName('orders');
  const productsTable = createTableName('products');
  const usersColumns = [
    { name: 'id', type: 'INTEGER' },
    { name: 'name', type: 'TEXT' },
    { name: 'role', type: 'TEXT' },
    { name: 'city', type: 'TEXT' },
    { name: 'team', type: 'TEXT' },
    { name: 'level', type: 'INTEGER' },
    { name: 'score', type: 'REAL' },
    { name: 'email', type: 'TEXT' },
    { name: 'status', type: 'TEXT' },
    { name: 'note', type: 'TEXT' },
  ];
  const ordersColumns = [
    { name: 'id', type: 'INTEGER' },
    { name: 'user_id', type: 'INTEGER' },
    { name: 'amount', type: 'REAL' },
    { name: 'currency', type: 'TEXT' },
    { name: 'region', type: 'TEXT' },
    { name: 'channel', type: 'TEXT' },
    { name: 'status', type: 'TEXT' },
    { name: 'remark', type: 'TEXT' },
    { name: 'created_day', type: 'TEXT' },
    { name: 'priority', type: 'INTEGER' },
  ];
  const productsColumns = [
    { name: 'id', type: 'INTEGER' },
    { name: 'name', type: 'TEXT' },
    { name: 'price', type: 'REAL' },
    { name: 'category', type: 'TEXT' },
    { name: 'brand', type: 'TEXT' },
    { name: 'sku', type: 'TEXT' },
    { name: 'stock', type: 'INTEGER' },
    { name: 'rating', type: 'REAL' },
    { name: 'shelf', type: 'TEXT' },
    { name: 'summary', type: 'TEXT' },
  ];

  await assertExecuteOk(
    createTableSql(usersTable, usersColumns),
    'create users table'
  );
  await assertExecuteOk(
    createTableSql(ordersTable, ordersColumns),
    'create orders table'
  );
  await assertExecuteOk(
    createTableSql(productsTable, productsColumns),
    'create products table'
  );

  for (let index = 1; index <= 100; index += 1) {
    await assertExecuteOk(
      insertSql(usersTable, usersColumns, [
        index,
        `Alice-${index}`,
        index % 2 === 0 ? 'admin' : 'member',
        `City-${index % 10}`,
        `Team-${index % 6}`,
        index,
        index + 0.5,
        `alice${index}@example.com`,
        index % 3 === 0 ? 'inactive' : 'active',
        `seed user ${index}`,
      ]),
      `insert user row ${index}`
    );

    await assertExecuteOk(
      insertSql(ordersTable, ordersColumns, [
        index,
        index,
        index * 10 + 0.25,
        'CNY',
        `REGION-${index % 5}`,
        index % 2 === 0 ? 'online' : 'offline',
        index % 4 === 0 ? 'pending' : 'paid',
        `priority order ${index}`,
        `2026-03-${String((index % 28) + 1).padStart(2, '0')}`,
        (index % 5) + 1,
      ]),
      `insert order row ${index}`
    );

    await assertExecuteOk(
      insertSql(productsTable, productsColumns, [
        index,
        `Keyboard-${index}`,
        index * 3 + 0.75,
        `category-${index % 4}`,
        `brand-${index % 7}`,
        `KB-${String(index).padStart(3, '0')}`,
        index * 2,
        (index % 5) + 0.8,
        `A-${String(index).padStart(2, '0')}`,
        `mechanical keyboard ${index}`,
      ]),
      `insert product row ${index}`
    );
  }

  const databaseInfo = await infoSql();
  assert.ok(databaseInfo.tables.includes(usersTable));
  assert.ok(databaseInfo.tables.includes(ordersTable));
  assert.ok(databaseInfo.tables.includes(productsTable));

  const userRows = await assertQueryOk(
    `SELECT id, name, role, city, team, level, score, email, status, note FROM ${usersTable} ORDER BY id`,
    'query users table'
  );
  assert.deepEqual(userRows.columns, ['id', 'name', 'role', 'city', 'team', 'level', 'score', 'email', 'status', 'note']);
  assert.equal(userRows.rows.length, 100);
  assert.deepEqual(userRows.rows[0], [
    { Integer: '1' },
    { Text: 'Alice-1' },
    { Text: 'member' },
    { Text: 'City-1' },
    { Text: 'Team-1' },
    { Integer: '1' },
    { Float: 1.5 },
    { Text: 'alice1@example.com' },
    { Text: 'active' },
    { Text: 'seed user 1' },
  ]);
  assert.deepEqual(userRows.rows[49], [
    { Integer: '50' },
    { Text: 'Alice-50' },
    { Text: 'admin' },
    { Text: 'City-0' },
    { Text: 'Team-2' },
    { Integer: '50' },
    { Float: 50.5 },
    { Text: 'alice50@example.com' },
    { Text: 'active' },
    { Text: 'seed user 50' },
  ]);
  assert.deepEqual(userRows.rows[99], [
    { Integer: '100' },
    { Text: 'Alice-100' },
    { Text: 'admin' },
    { Text: 'City-0' },
    { Text: 'Team-4' },
    { Integer: '100' },
    { Float: 100.5 },
    { Text: 'alice100@example.com' },
    { Text: 'active' },
    { Text: 'seed user 100' },
  ]);

  const orderRows = await assertQueryOk(
    `SELECT id, user_id, amount, currency, region, channel, status, remark, created_day, priority FROM ${ordersTable} ORDER BY id`,
    'query orders table'
  );
  assert.deepEqual(orderRows.columns, ['id', 'user_id', 'amount', 'currency', 'region', 'channel', 'status', 'remark', 'created_day', 'priority']);
  assert.equal(orderRows.rows.length, 100);
  assert.deepEqual(orderRows.rows[0], [
    { Integer: '1' },
    { Integer: '1' },
    { Float: 10.25 },
    { Text: 'CNY' },
    { Text: 'REGION-1' },
    { Text: 'offline' },
    { Text: 'paid' },
    { Text: 'priority order 1' },
    { Text: '2026-03-02' },
    { Integer: '2' },
  ]);
  assert.deepEqual(orderRows.rows[49], [
    { Integer: '50' },
    { Integer: '50' },
    { Float: 500.25 },
    { Text: 'CNY' },
    { Text: 'REGION-0' },
    { Text: 'online' },
    { Text: 'paid' },
    { Text: 'priority order 50' },
    { Text: '2026-03-23' },
    { Integer: '1' },
  ]);
  assert.deepEqual(orderRows.rows[99], [
    { Integer: '100' },
    { Integer: '100' },
    { Float: 1000.25 },
    { Text: 'CNY' },
    { Text: 'REGION-0' },
    { Text: 'online' },
    { Text: 'pending' },
    { Text: 'priority order 100' },
    { Text: '2026-03-17' },
    { Integer: '1' },
  ]);

  const productRows = await assertQueryOk(
    `SELECT id, name, price, category, brand, sku, stock, rating, shelf, summary FROM ${productsTable} ORDER BY id`,
    'query products table'
  );
  assert.deepEqual(productRows.columns, ['id', 'name', 'price', 'category', 'brand', 'sku', 'stock', 'rating', 'shelf', 'summary']);
  assert.equal(productRows.rows.length, 100);
  assert.deepEqual(productRows.rows[0], [
    { Integer: '1' },
    { Text: 'Keyboard-1' },
    { Float: 3.75 },
    { Text: 'category-1' },
    { Text: 'brand-1' },
    { Text: 'KB-001' },
    { Integer: '2' },
    { Float: 1.8 },
    { Text: 'A-01' },
    { Text: 'mechanical keyboard 1' },
  ]);
  assert.deepEqual(productRows.rows[49], [
    { Integer: '50' },
    { Text: 'Keyboard-50' },
    { Float: 150.75 },
    { Text: 'category-2' },
    { Text: 'brand-1' },
    { Text: 'KB-050' },
    { Integer: '100' },
    { Float: 0.8 },
    { Text: 'A-50' },
    { Text: 'mechanical keyboard 50' },
  ]);
  assert.deepEqual(productRows.rows[99], [
    { Integer: '100' },
    { Text: 'Keyboard-100' },
    { Float: 300.75 },
    { Text: 'category-0' },
    { Text: 'brand-2' },
    { Text: 'KB-100' },
    { Integer: '200' },
    { Float: 0.8 },
    { Text: 'A-100' },
    { Text: 'mechanical keyboard 100' },
  ]);
}

async function testTypedQueryResults() {
  const tableName = createTableName('records');

  await assertExecuteOk(
    `CREATE TABLE ${tableName} (id INTEGER, name TEXT, score REAL, note TEXT)`,
    'create records table'
  );
  await assertExecuteOk(
    `INSERT INTO ${tableName} (id, name, score, note) VALUES (1, 'Alice', 9.5, NULL)`,
    'insert first record'
  );
  await assertExecuteOk(
    `INSERT INTO ${tableName} (id, name, score, note) VALUES (2, 'Bob', 7.25, 'ready')`,
    'insert second record'
  );

  const result = await assertQueryOk(
    `SELECT id, name, score, note FROM ${tableName} ORDER BY id`,
    'select inserted records'
  );

  assert.deepEqual(result.columns, ['id', 'name', 'score', 'note']);
  assert.deepEqual(result.rows, [
    [
      { Integer: '1' },
      { Text: 'Alice' },
      { Float: 9.5 },
      { Null: null },
    ],
    [
      { Integer: '2' },
      { Text: 'Bob' },
      { Float: 7.25 },
      { Text: 'ready' },
    ],
  ]);
}

async function testChineseTextAndIdentifiers() {
  const baseTableName = createTableName('用户表');
  const chineseColumns = [
    { name: '编号', type: 'INTEGER' },
    { name: '姓名', type: 'TEXT' },
    { name: '城市', type: 'TEXT' },
    { name: '部门', type: 'TEXT' },
    { name: '职位', type: 'TEXT' },
    { name: '备注', type: 'TEXT' },
    { name: '标签', type: 'TEXT' },
    { name: '状态', type: 'TEXT' },
    { name: '积分', type: 'REAL' },
    { name: '说明', type: 'TEXT' },
  ];

  await assertExecuteOk(
    createTableSql(baseTableName, chineseColumns),
    'create chinese table'
  );

  for (let index = 1; index <= 100; index += 1) {
    await assertExecuteOk(
      insertSql(baseTableName, chineseColumns, [
        index,
        `中文用户${index}`,
        `城市${index % 10}`,
        `部门${index % 5}`,
        `职位${index % 8}`,
        `备注${index}`,
        `标签${index % 6}`,
        index % 2 === 0 ? '启用' : '禁用',
        index + 0.5,
        `第${index}条中文说明`,
      ]),
      `insert chinese row ${index}`
    );
  }

  const databaseInfo = await infoSql();
  assert.ok(databaseInfo.tables.includes(baseTableName));

  const insertedRows = await assertQueryOk(
    `SELECT 编号, 姓名, 城市, 部门, 职位, 备注, 标签, 状态, 积分, 说明 FROM ${baseTableName} ORDER BY 编号`,
    'query chinese rows'
  );

  assert.deepEqual(insertedRows.columns, ['编号', '姓名', '城市', '部门', '职位', '备注', '标签', '状态', '积分', '说明']);
  assert.equal(insertedRows.rows.length, 100);
  assert.deepEqual(insertedRows.rows[0], [
    { Integer: '1' },
    { Text: '中文用户1' },
    { Text: '城市1' },
    { Text: '部门1' },
    { Text: '职位1' },
    { Text: '备注1' },
    { Text: '标签1' },
    { Text: '禁用' },
    { Float: 1.5 },
    { Text: '第1条中文说明' },
  ]);
  assert.deepEqual(insertedRows.rows[49], [
    { Integer: '50' },
    { Text: '中文用户50' },
    { Text: '城市0' },
    { Text: '部门0' },
    { Text: '职位2' },
    { Text: '备注50' },
    { Text: '标签2' },
    { Text: '启用' },
    { Float: 50.5 },
    { Text: '第50条中文说明' },
  ]);
  assert.deepEqual(insertedRows.rows[99], [
    { Integer: '100' },
    { Text: '中文用户100' },
    { Text: '城市0' },
    { Text: '部门0' },
    { Text: '职位4' },
    { Text: '备注100' },
    { Text: '标签4' },
    { Text: '启用' },
    { Float: 100.5 },
    { Text: '第100条中文说明' },
  ]);

  await assertExecuteOk(
    `UPDATE ${baseTableName} SET 城市 = '深圳', 备注 = '严格中文检查', 状态 = '已更新' WHERE 编号 = 50`,
    'update chinese row'
  );

  const updatedRows = await assertQueryOk(
    `SELECT 编号, 姓名, 城市, 部门, 职位, 备注, 标签, 状态, 积分, 说明 FROM ${baseTableName} WHERE 编号 = 50 ORDER BY 编号`,
    'query updated chinese rows'
  );
  assert.deepEqual(updatedRows.rows, [[
    { Integer: '50' },
    { Text: '中文用户50' },
    { Text: '深圳' },
    { Text: '部门0' },
    { Text: '职位2' },
    { Text: '严格中文检查' },
    { Text: '标签2' },
    { Text: '已更新' },
    { Float: 50.5 },
    { Text: '第50条中文说明' },
  ]]);

  await assertExecuteOk(
    `DELETE FROM ${baseTableName} WHERE 编号 = 100`,
    'delete chinese row'
  );

  const remainingRows = await assertQueryOk(
    `SELECT 编号, 姓名, 城市, 部门, 职位, 备注, 标签, 状态, 积分, 说明 FROM ${baseTableName} ORDER BY 编号`,
    'query remaining chinese rows'
  );
  assert.equal(remainingRows.rows.length, 99);
  assert.deepEqual(remainingRows.rows.at(-1), [
    { Integer: '99' },
    { Text: '中文用户99' },
    { Text: '城市9' },
    { Text: '部门4' },
    { Text: '职位3' },
    { Text: '备注99' },
    { Text: '标签3' },
    { Text: '禁用' },
    { Float: 99.5 },
    { Text: '第99条中文说明' },
  ]);
}

async function testChineseUpgradePersistence() {
  const tableName = createTableName('升级中文');
  const columns = [
    { name: '编号', type: 'INTEGER' },
    { name: '内容', type: 'TEXT' },
    { name: '城市', type: 'TEXT' },
    { name: '部门', type: 'TEXT' },
    { name: '职位', type: 'TEXT' },
    { name: '备注', type: 'TEXT' },
    { name: '标签', type: 'TEXT' },
    { name: '状态', type: 'TEXT' },
    { name: '积分', type: 'REAL' },
    { name: '说明', type: 'TEXT' },
  ];

  await assertExecuteOk(
    createTableSql(tableName, columns),
    'create chinese upgrade table'
  );
  await assertExecuteOk(
    insertSql(tableName, columns, [
      1,
      '升级前中文数据',
      '杭州',
      '平台部',
      '工程师',
      '升级校验',
      '持久化',
      '有效',
      88.8,
      '升级后的中文列需要完整保留',
    ]),
    'insert chinese upgrade row'
  );

  const beforeUpgrade = await assertQueryOk(
    `SELECT 编号, 内容, 城市, 部门, 职位, 备注, 标签, 状态, 积分, 说明 FROM ${tableName} ORDER BY 编号`,
    'query chinese rows before upgrade'
  );
  assert.deepEqual(beforeUpgrade.rows, [[
    { Integer: '1' },
    { Text: '升级前中文数据' },
    { Text: '杭州' },
    { Text: '平台部' },
    { Text: '工程师' },
    { Text: '升级校验' },
    { Text: '持久化' },
    { Text: '有效' },
    { Float: 88.8 },
    { Text: '升级后的中文列需要完整保留' },
  ]]);

  runDfx(['canister', 'install', 'backend', '--mode', 'upgrade'], {
    stdio: 'inherit',
  });

  const databaseInfo = await infoSql();
  assert.ok(databaseInfo.tables.includes(tableName));

  const afterUpgrade = await assertQueryOk(
    `SELECT 编号, 内容, 城市, 部门, 职位, 备注, 标签, 状态, 积分, 说明 FROM ${tableName} ORDER BY 编号`,
    'query chinese rows after upgrade'
  );
  assert.deepEqual(afterUpgrade.columns, ['编号', '内容', '城市', '部门', '职位', '备注', '标签', '状态', '积分', '说明']);
  assert.deepEqual(afterUpgrade.rows, [[
    { Integer: '1' },
    { Text: '升级前中文数据' },
    { Text: '杭州' },
    { Text: '平台部' },
    { Text: '工程师' },
    { Text: '升级校验' },
    { Text: '持久化' },
    { Text: '有效' },
    { Float: 88.8 },
    { Text: '升级后的中文列需要完整保留' },
  ]]);
}

async function testEmptyResultColumns() {
  const tableName = createTableName('empty_result');

  await assertExecuteOk(
    `CREATE TABLE ${tableName} (id INTEGER, title TEXT)`,
    'create empty result table'
  );
  await assertExecuteOk(
    `INSERT INTO ${tableName} (id, title) VALUES (1, 'present')`,
    'insert seed row'
  );

  const result = await assertQueryOk(
    `SELECT id, title FROM ${tableName} WHERE id = 999`,
    'query empty result set'
  );

  assert.deepEqual(result.columns, ['id', 'title']);
  assert.deepEqual(result.rows, []);
}

async function testUpdateAndDelete() {
  const tableName = createTableName('mutations');

  await assertExecuteOk(
    `CREATE TABLE ${tableName} (id INTEGER, name TEXT, age INTEGER)`,
    'create mutations table'
  );
  await assertExecuteOk(
    `INSERT INTO ${tableName} (id, name, age) VALUES (1, 'Alice', 30)`,
    'insert alice'
  );
  await assertExecuteOk(
    `INSERT INTO ${tableName} (id, name, age) VALUES (2, 'Bob', 40)`,
    'insert bob'
  );
  await assertExecuteOk(
    `UPDATE ${tableName} SET name = 'Alice Updated', age = 31 WHERE id = 1`,
    'update alice'
  );
  await assertExecuteOk(
    `DELETE FROM ${tableName} WHERE id = 2`,
    'delete bob'
  );

  const result = await assertQueryOk(
    `SELECT id, name, age FROM ${tableName} ORDER BY id`,
    'query remaining rows after mutations'
  );

  assert.deepEqual(result.columns, ['id', 'name', 'age']);
  assert.deepEqual(result.rows, [[
    { Integer: '1' },
    { Text: 'Alice Updated' },
    { Integer: '31' },
  ]]);
}

async function testStoreTableInDatabase() {
  const tableName = createTableName('stored_table');

  await assertExecuteOk(
    `CREATE TABLE ${tableName} (id INTEGER, name TEXT, note TEXT)`,
    'create stored table'
  );
  await assertExecuteOk(
    `INSERT INTO ${tableName} (id, name, note) VALUES (1, 'Saved Row', 'stored in database')`,
    'insert stored row'
  );

  const databaseInfo = await infoSql();
  assert.ok(databaseInfo.tables.includes(tableName));

  const rows = await assertQueryOk(
    `SELECT id, name, note FROM ${tableName} ORDER BY id`,
    'query stored table'
  );
  assert.deepEqual(rows.columns, ['id', 'name', 'note']);
  assert.deepEqual(rows.rows, [[
    { Integer: '1' },
    { Text: 'Saved Row' },
    { Text: 'stored in database' },
  ]]);
}

async function testTransactionRollback() {
  const tableName = createTableName('txn_rollback');

  await assertExecuteOk(
    `CREATE TABLE ${tableName} (id INTEGER, note TEXT)`,
    'create rollback table'
  );
  await assertExecuteOk('BEGIN TRANSACTION', 'begin rollback transaction');
  await assertExecuteOk(
    `INSERT INTO ${tableName} (id, note) VALUES (1, 'rolled-back')`,
    'insert row in rollback transaction'
  );

  const beforeRollback = await assertQueryOk(
    `SELECT id, note FROM ${tableName} ORDER BY id`,
    'query rows before rollback'
  );
  assert.deepEqual(beforeRollback.rows, [[
    { Integer: '1' },
    { Text: 'rolled-back' },
  ]]);

  await assertExecuteOk('ROLLBACK', 'rollback transaction');

  const afterRollback = await assertQueryOk(
    `SELECT id, note FROM ${tableName} ORDER BY id`,
    'query rows after rollback'
  );
  assert.deepEqual(afterRollback.columns, ['id', 'note']);
  assert.deepEqual(afterRollback.rows, []);
}

async function testTransactionCommit() {
  const tableName = createTableName('txn_commit');

  await assertExecuteOk(
    `CREATE TABLE ${tableName} (id INTEGER, note TEXT)`,
    'create commit table'
  );
  await assertExecuteOk('BEGIN TRANSACTION', 'begin commit transaction');
  await assertExecuteOk(
    `INSERT INTO ${tableName} (id, note) VALUES (1, 'committed')`,
    'insert row in commit transaction'
  );
  await assertExecuteOk('COMMIT', 'commit transaction');

  const afterCommit = await assertQueryOk(
    `SELECT id, note FROM ${tableName} ORDER BY id`,
    'query rows after commit'
  );
  assert.deepEqual(afterCommit.rows, [[
    { Integer: '1' },
    { Text: 'committed' },
  ]]);
}

async function testBatchExecuteStatements() {
  const tableName = createTableName('batch_exec');

  const result = await assertBatchExecuteOk([
    `CREATE TABLE ${tableName} (id INTEGER, name TEXT)`,
    'BEGIN TRANSACTION',
    `INSERT INTO ${tableName} (id, name) VALUES (1, 'batch-first')`,
    `INSERT INTO ${tableName} (id, name) VALUES (2, 'batch-second')`,
    'COMMIT',
    `SELECT id, name FROM ${tableName} ORDER BY id`,
  ], 'execute batch statements');

  assert.equal(result.statements_executed, 6);
  assert.equal(result.changed_schema_or_data, true);
  assert.equal(result.has_query_result, true);
  assert.deepEqual(result.last_query_result.columns, ['id', 'name']);
  assert.deepEqual(result.last_query_result.rows, [
    [
      { Integer: '1' },
      { Text: 'batch-first' },
    ],
    [
      { Integer: '2' },
      { Text: 'batch-second' },
    ],
  ]);
}

async function testUnsupportedExecuteStatements() {
  const unsupportedErr = unwrapErr(
    await executeSql('PRAGMA cache_size = 1000', 'execute unsupported pragma'),
    'execute unsupported pragma'
  );
  assert.ok(unsupportedErr.startsWith('unsupported execute statement:'));
}

async function testStatementValidation() {
  const selectErr = unwrapErr(
    await executeSql('SELECT 1', 'execute select statement'),
    'execute select statement'
  );
  assert.equal(
    selectErr,
    '`execute` does not support SELECT statements, use `query` instead'
  );

  const writeErr = unwrapErr(
    await querySql('CREATE TABLE should_fail (id INTEGER)', 'query create table'),
    'query create table'
  );
  assert.equal(writeErr, '`query` only supports SELECT statements');
}

async function testErrorSurfacing() {
  const invalidExecuteErr = unwrapErr(
    await executeSql('CREAT TABLE broken (id INTEGER)', 'invalid execute sql'),
    'invalid execute sql'
  );
  assert.ok(invalidExecuteErr.length > 0);

  const missingTableErr = unwrapErr(
    await querySql(
      `SELECT id FROM ${createTableName('missing')}`,
      'query missing table'
    ),
    'query missing table'
  );
  assert.ok(missingTableErr.length > 0);
}

async function testUpgradePersistence() {
  const tableName = createTableName('upgrade_state');

  await assertExecuteOk(
    `CREATE TABLE ${tableName} (id INTEGER, name TEXT)`,
    'create upgrade state table'
  );
  await assertExecuteOk(
    `INSERT INTO ${tableName} (id, name) VALUES (1, 'before-upgrade')`,
    'insert upgrade state row'
  );

  const beforeUpgrade = await assertQueryOk(
    `SELECT id, name FROM ${tableName} ORDER BY id`,
    'query before upgrade'
  );
  assert.deepEqual(beforeUpgrade.rows, [[
    { Integer: '1' },
    { Text: 'before-upgrade' },
  ]]);

  runDfx(['canister', 'install', 'backend', '--mode', 'upgrade'], {
    stdio: 'inherit',
  });

  const databaseInfo = await infoSql();
  assert.ok(databaseInfo.tables.includes(tableName));

  const afterUpgrade = await assertQueryOk(
    `SELECT id, name FROM ${tableName} ORDER BY id`,
    'query after upgrade'
  );

  assert.deepEqual(afterUpgrade.columns, ['id', 'name']);
  assert.deepEqual(afterUpgrade.rows, [[
    { Integer: '1' },
    { Text: 'before-upgrade' },
  ]]);
}

async function main() {
  resetLocalReplica();

  runDfx(['deploy', 'backend'], {
    stdio: 'inherit',
  });

  await runCase('fresh info is empty', testFreshInfo);
  await runCase('info returns sorted tables', testInfoSorting);
  await runCase('create three tables and insert rows', testCreateTablesAndInsertRows);
  await runCase('query returns typed values', testTypedQueryResults);
  await runCase('supports chinese text and identifiers across CRUD', testChineseTextAndIdentifiers);
  await runCase('empty result preserves columns', testEmptyResultColumns);
  await runCase('update and delete mutations', testUpdateAndDelete);
  await runCase('store table in database', testStoreTableInDatabase);
  await runCase('transaction rollback restores state', testTransactionRollback);
  await runCase('transaction commit persists state', testTransactionCommit);
  await runCase('batch execute handles mixed statements', testBatchExecuteStatements);
  await runCase('statement validation errors', testStatementValidation);
  await runCase('unsupported execute statements return errors', testUnsupportedExecuteStatements);
  await runCase('parser and runtime errors surface', testErrorSurfacing);
  await runCase('upgrade preserves chinese data', testChineseUpgradePersistence);
  await runCase('upgrade preserves data', testUpgradePersistence);

  console.log('All canister integration checks passed');
}

await main();