import {
  assert,
  runDfx,
  infoSql,
  createTableName,
  assertExecuteOk,
  createTableSql,
  insertSql,
  assertQueryOk,
  assertBatchExecuteOk,
  unwrapErr,
  executeSql,
  querySql,
} from '../harness.js';

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

export const lifecycleTestCases = [
  { fullName: 'testChineseTextAndIdentifiers', displayName: 'supports chinese text and identifiers across CRUD', fn: testChineseTextAndIdentifiers },
  { fullName: 'testEmptyResultColumns', displayName: 'empty result preserves columns', fn: testEmptyResultColumns },
  { fullName: 'testUpdateAndDelete', displayName: 'update and delete mutations', fn: testUpdateAndDelete },
  { fullName: 'testStoreTableInDatabase', displayName: 'store table in database', fn: testStoreTableInDatabase },
  { fullName: 'testTransactionRollback', displayName: 'transaction rollback restores state', fn: testTransactionRollback },
  { fullName: 'testTransactionCommit', displayName: 'transaction commit persists state', fn: testTransactionCommit },
  { fullName: 'testBatchExecuteStatements', displayName: 'batch execute handles mixed statements', fn: testBatchExecuteStatements },
  { fullName: 'testStatementValidation', displayName: 'statement validation errors', fn: testStatementValidation },
  { fullName: 'testUnsupportedExecuteStatements', displayName: 'unsupported execute statements return errors', fn: testUnsupportedExecuteStatements },
  { fullName: 'testErrorSurfacing', displayName: 'parser and runtime errors surface', fn: testErrorSurfacing },
  { fullName: 'testChineseUpgradePersistence', displayName: 'upgrade preserves chinese data', fn: testChineseUpgradePersistence },
  { fullName: 'testUpgradePersistence', displayName: 'upgrade preserves data', fn: testUpgradePersistence },
];