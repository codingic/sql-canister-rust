import {
  assert,
  createTableName,
  assertExecuteOk,
  executeSql,
  querySql,
  assertQueryOk,
  unwrapErr,
  infoSql,
} from '../harness.js';

async function testConstraintEnforcement() {
  const tableName = createTableName('constraints');

  await assertExecuteOk(
    `CREATE TABLE ${tableName} (id INTEGER PRIMARY KEY, name TEXT NOT NULL, email TEXT UNIQUE, age INTEGER DEFAULT 18)`,
    'create constraints table'
  );

  await assertExecuteOk(
    `INSERT INTO ${tableName} (id, name, email) VALUES (1, 'Alice', 'alice@example.com')`,
    'insert constrained row with default age'
  );

  const seededRow = await assertQueryOk(
    `SELECT id, name, email, age FROM ${tableName} ORDER BY id`,
    'query constrained row with default age'
  );
  assert.deepEqual(seededRow.rows, [[
    { Integer: '1' },
    { Text: 'Alice' },
    { Text: 'alice@example.com' },
    { Integer: '18' },
  ]]);

  const nullNameErr = unwrapErr(
    await executeSql(
      `INSERT INTO ${tableName} (id, name, email) VALUES (2, NULL, 'null-name@example.com')`,
      'insert null into NOT NULL column'
    ),
    'insert null into NOT NULL column'
  );
  assert.ok(nullNameErr.includes('NOT NULL constraint failed'));

  const duplicatePkErr = unwrapErr(
    await executeSql(
      `INSERT INTO ${tableName} (id, name, email) VALUES (1, 'Bob', 'bob@example.com')`,
      'insert duplicate primary key'
    ),
    'insert duplicate primary key'
  );
  assert.ok(duplicatePkErr.includes('PRIMARY KEY constraint failed'));

  await assertExecuteOk(
    `INSERT INTO ${tableName} (id, name, email, age) VALUES (2, 'Bob', 'bob@example.com', 20)`,
    'insert second constrained row'
  );

  const duplicateUniqueErr = unwrapErr(
    await executeSql(
      `INSERT INTO ${tableName} (id, name, email) VALUES (3, 'Carol', 'alice@example.com')`,
      'insert duplicate unique email'
    ),
    'insert duplicate unique email'
  );
  assert.ok(duplicateUniqueErr.includes('UNIQUE constraint failed'));

  const updateUniqueErr = unwrapErr(
    await executeSql(
      `UPDATE ${tableName} SET email = 'alice@example.com' WHERE id = 2`,
      'update to duplicate unique email'
    ),
    'update to duplicate unique email'
  );
  assert.ok(updateUniqueErr.includes('UNIQUE constraint failed'));

  const updateNullErr = unwrapErr(
    await executeSql(
      `UPDATE ${tableName} SET name = NULL WHERE id = 2`,
      'update not null column to null'
    ),
    'update not null column to null'
  );
  assert.ok(updateNullErr.includes('NOT NULL constraint failed'));
}

async function testPrimaryKeyOperations() {
  const tableName = createTableName('primary_key_ops');

  await assertExecuteOk(
    `CREATE TABLE ${tableName} (id INTEGER PRIMARY KEY, name TEXT NOT NULL, city TEXT)`,
    'create primary key table'
  );

  await assertExecuteOk(
    `INSERT INTO ${tableName} (name, city) VALUES ('Alice', 'Shanghai')`,
    'insert first row without explicit primary key'
  );
  await assertExecuteOk(
    `INSERT INTO ${tableName} (name, city) VALUES ('Bob', 'Shenzhen')`,
    'insert second row without explicit primary key'
  );

  const allRows = await assertQueryOk(
    `SELECT id, name, city FROM ${tableName} ORDER BY id`,
    'query rows with generated primary keys'
  );
  assert.deepEqual(allRows.rows, [
    [
      { Integer: '1' },
      { Text: 'Alice' },
      { Text: 'Shanghai' },
    ],
    [
      { Integer: '2' },
      { Text: 'Bob' },
      { Text: 'Shenzhen' },
    ],
  ]);

  const rowByPk = await assertQueryOk(
    `SELECT name, city FROM ${tableName} WHERE id = 2`,
    'query row by primary key'
  );
  assert.deepEqual(rowByPk.rows, [[
    { Text: 'Bob' },
    { Text: 'Shenzhen' },
  ]]);

  const rowByPkAlias = await assertQueryOk(
    `SELECT t.name FROM ${tableName} AS t WHERE t.id = 1`,
    'query row by aliased primary key'
  );
  assert.deepEqual(rowByPkAlias.rows, [[{ Text: 'Alice' }]]);

  await assertExecuteOk(
    `UPDATE ${tableName} SET city = 'Hangzhou' WHERE id = 2`,
    'update row by primary key'
  );

  const updatedRow = await assertQueryOk(
    `SELECT id, name, city FROM ${tableName} WHERE id = 2`,
    'query updated row by primary key'
  );
  assert.deepEqual(updatedRow.rows, [[
    { Integer: '2' },
    { Text: 'Bob' },
    { Text: 'Hangzhou' },
  ]]);

  await assertExecuteOk(
    `DELETE FROM ${tableName} WHERE id = 1`,
    'delete row by primary key'
  );

  const remainingRows = await assertQueryOk(
    `SELECT id, name, city FROM ${tableName} ORDER BY id`,
    'query rows after primary key delete'
  );
  assert.deepEqual(remainingRows.rows, [[
    { Integer: '2' },
    { Text: 'Bob' },
    { Text: 'Hangzhou' },
  ]]);
}

async function testInsertFromSelect() {
  const sourceTable = createTableName('insert_select_source');
  const targetTable = createTableName('insert_select_target');

  await assertExecuteOk(
    `CREATE TABLE ${sourceTable} (id INTEGER PRIMARY KEY, name TEXT NOT NULL, active INTEGER NOT NULL, city TEXT)`,
    'create insert-select source table'
  );
  await assertExecuteOk(
    `CREATE TABLE ${targetTable} (id INTEGER PRIMARY KEY, name TEXT NOT NULL, city TEXT DEFAULT 'unknown')`,
    'create insert-select target table'
  );

  await assertExecuteOk(
    `INSERT INTO ${sourceTable} (id, name, active, city) VALUES (1, 'Alice', 1, 'Shanghai')`,
    'seed insert-select source row 1'
  );
  await assertExecuteOk(
    `INSERT INTO ${sourceTable} (id, name, active, city) VALUES (2, 'Bob', 0, 'Shenzhen')`,
    'seed insert-select source row 2'
  );
  await assertExecuteOk(
    `INSERT INTO ${sourceTable} (id, name, active, city) VALUES (3, 'Carol', 1, 'Hangzhou')`,
    'seed insert-select source row 3'
  );

  await assertExecuteOk(
    `INSERT INTO ${targetTable} (id, name) SELECT id, name FROM ${sourceTable} WHERE active = 1 ORDER BY id`,
    'insert filtered rows from select'
  );

  const insertedRows = await assertQueryOk(
    `SELECT id, name, city FROM ${targetTable} ORDER BY id`,
    'query rows inserted from select'
  );
  assert.deepEqual(insertedRows.rows, [
    [
      { Integer: '1' },
      { Text: 'Alice' },
      { Text: 'unknown' },
    ],
    [
      { Integer: '3' },
      { Text: 'Carol' },
      { Text: 'unknown' },
    ],
  ]);

  await assertExecuteOk(
    `INSERT INTO ${targetTable} (id, name, city) SELECT id + 10, name, city FROM ${sourceTable} WHERE active = 0`,
    'insert projected rows from select'
  );

  const projectedRows = await assertQueryOk(
    `SELECT id, name, city FROM ${targetTable} ORDER BY id`,
    'query projected insert-select rows'
  );
  assert.deepEqual(projectedRows.rows, [
    [
      { Integer: '1' },
      { Text: 'Alice' },
      { Text: 'unknown' },
    ],
    [
      { Integer: '3' },
      { Text: 'Carol' },
      { Text: 'unknown' },
    ],
    [
      { Integer: '12' },
      { Text: 'Bob' },
      { Text: 'Shenzhen' },
    ],
  ]);

  await assertExecuteOk(
    `INSERT INTO ${targetTable} (id, name) SELECT id + 100, name FROM ${sourceTable} WHERE active = 1 UNION ALL SELECT id + 200, name FROM ${sourceTable} WHERE active = 0 ORDER BY id`,
    'insert rows from compound select'
  );

  const compoundRows = await assertQueryOk(
    `SELECT id, name FROM ${targetTable} WHERE id >= 100 ORDER BY id`,
    'query rows inserted from compound select'
  );
  assert.deepEqual(compoundRows.rows, [
    [
      { Integer: '101' },
      { Text: 'Alice' },
    ],
    [
      { Integer: '103' },
      { Text: 'Carol' },
    ],
    [
      { Integer: '202' },
      { Text: 'Bob' },
    ],
  ]);

  const mismatchedColumnsErr = unwrapErr(
    await executeSql(
      `INSERT INTO ${targetTable} (id, name) SELECT id FROM ${sourceTable}`,
      'insert from select with mismatched column count'
    ),
    'insert from select with mismatched column count'
  );
  assert.ok(
    mismatchedColumnsErr.includes('INSERT INTO ... SELECT returned 1 columns but 2 target columns were specified')
  );
}

async function testCheckConstraints() {
  const tableName = createTableName('check_constraints');

  await assertExecuteOk(
    `CREATE TABLE ${tableName} (id INTEGER PRIMARY KEY, age INTEGER CHECK (age >= 18), score REAL CHECK (score <= 100), name TEXT CHECK (name <> 'blocked'))`,
    'create table with check constraints'
  );

  await assertExecuteOk(
    `INSERT INTO ${tableName} (id, age, score, name) VALUES (1, 20, 88.5, 'Alice')`,
    'insert valid row with check constraints'
  );

  await assertExecuteOk(
    `INSERT INTO ${tableName} (id, age, score, name) VALUES (2, NULL, NULL, NULL)`,
    'insert null values allowed by check constraints'
  );

  const ageErr = unwrapErr(
    await executeSql(
      `INSERT INTO ${tableName} (id, age, score, name) VALUES (3, 17, 90, 'Bob')`,
      'insert invalid age check'
    ),
    'insert invalid age check'
  );
  assert.ok(ageErr.includes('CHECK constraint failed'));

  const scoreErr = unwrapErr(
    await executeSql(
      `INSERT INTO ${tableName} (id, age, score, name) VALUES (4, 22, 101, 'Carol')`,
      'insert invalid score check'
    ),
    'insert invalid score check'
  );
  assert.ok(scoreErr.includes('CHECK constraint failed'));

  const nameErr = unwrapErr(
    await executeSql(
      `UPDATE ${tableName} SET name = 'blocked' WHERE id = 1`,
      'update invalid name check'
    ),
    'update invalid name check'
  );
  assert.ok(nameErr.includes('CHECK constraint failed'));
}

async function testUpsertOperations() {
  const tableName = createTableName('upsert');

  await assertExecuteOk(
    `CREATE TABLE ${tableName} (id INTEGER PRIMARY KEY, email TEXT UNIQUE, visits INTEGER NOT NULL DEFAULT 0, city TEXT)`,
    'create upsert table'
  );

  await assertExecuteOk(
    `INSERT INTO ${tableName} (id, email, visits, city) VALUES (1, 'alice@example.com', 1, 'Shanghai')`,
    'seed upsert base row'
  );

  await assertExecuteOk(
    `INSERT INTO ${tableName} (id, email, visits, city) VALUES (1, 'ignored@example.com', 99, 'Beijing') ON CONFLICT(id) DO NOTHING`,
    'upsert do nothing on primary key conflict'
  );

  const afterDoNothing = await assertQueryOk(
    `SELECT id, email, visits, city FROM ${tableName} ORDER BY id`,
    'query row after do nothing'
  );
  assert.deepEqual(afterDoNothing.rows, [[
    { Integer: '1' },
    { Text: 'alice@example.com' },
    { Integer: '1' },
    { Text: 'Shanghai' },
  ]]);

  await assertExecuteOk(
    `INSERT INTO ${tableName} (id, email, visits, city) VALUES (2, 'alice@example.com', 4, 'Hangzhou') ON CONFLICT(email) DO UPDATE SET visits = visits + excluded.visits, city = excluded.city`,
    'upsert do update on unique email conflict'
  );

  const afterDoUpdate = await assertQueryOk(
    `SELECT id, email, visits, city FROM ${tableName} ORDER BY id`,
    'query row after do update'
  );
  assert.deepEqual(afterDoUpdate.rows, [[
    { Integer: '1' },
    { Text: 'alice@example.com' },
    { Integer: '5' },
    { Text: 'Hangzhou' },
  ]]);

  await assertExecuteOk(
    `INSERT INTO ${tableName} (id, email, visits, city) VALUES (3, 'alice@example.com', 2, 'Suzhou') ON CONFLICT(email) DO UPDATE SET city = excluded.city WHERE excluded.visits > ${tableName}.visits`,
    'upsert do update where can skip update'
  );

  const afterSkippedUpdate = await assertQueryOk(
    `SELECT id, email, visits, city FROM ${tableName} ORDER BY id`,
    'query row after skipped upsert update'
  );
  assert.deepEqual(afterSkippedUpdate.rows, [[
    { Integer: '1' },
    { Text: 'alice@example.com' },
    { Integer: '5' },
    { Text: 'Hangzhou' },
  ]]);

  const invalidTargetErr = unwrapErr(
    await executeSql(
      `INSERT INTO ${tableName} (id, email, visits, city) VALUES (4, 'bob@example.com', 1, 'Nanjing') ON CONFLICT(city) DO NOTHING`,
      'upsert invalid conflict target'
    ),
    'upsert invalid conflict target'
  );
  assert.ok(invalidTargetErr.includes('ON CONFLICT column is not UNIQUE or PRIMARY KEY'));
}

async function testAlterTableFeatures() {
  const originalTable = createTableName('alter_table');
  const renamedTable = `${originalTable}_renamed`;

  await assertExecuteOk(
    `CREATE TABLE ${originalTable} (id INTEGER PRIMARY KEY, name TEXT)`,
    'create alter table source table'
  );
  await assertExecuteOk(
    `INSERT INTO ${originalTable} (id, name) VALUES (1, 'Alice')`,
    'insert alter table seed row'
  );

  await assertExecuteOk(
    `ALTER TABLE ${originalTable} RENAME TO ${renamedTable}`,
    'rename table'
  );

  const infoAfterRename = await infoSql();
  assert.ok(infoAfterRename.tables.includes(renamedTable));
  assert.ok(!infoAfterRename.tables.includes(originalTable));

  await assertExecuteOk(
    `ALTER TABLE ${renamedTable} RENAME COLUMN name TO display_name`,
    'rename column'
  );

  await assertExecuteOk(
    `ALTER TABLE ${renamedTable} ADD COLUMN status TEXT NOT NULL DEFAULT 'active'`,
    'add column with default'
  );

  const rowsAfterAlter = await assertQueryOk(
    `SELECT id, display_name, status FROM ${renamedTable} ORDER BY id`,
    'query rows after alter table'
  );
  assert.deepEqual(rowsAfterAlter.columns, ['id', 'display_name', 'status']);
  assert.deepEqual(rowsAfterAlter.rows, [[
    { Integer: '1' },
    { Text: 'Alice' },
    { Text: 'active' },
  ]]);

  await assertExecuteOk(
    `INSERT INTO ${renamedTable} (id, display_name) VALUES (2, 'Bob')`,
    'insert row after add column'
  );

  const rowsWithDefault = await assertQueryOk(
    `SELECT id, display_name, status FROM ${renamedTable} ORDER BY id`,
    'query defaults after add column'
  );
  assert.deepEqual(rowsWithDefault.rows, [
    [
      { Integer: '1' },
      { Text: 'Alice' },
      { Text: 'active' },
    ],
    [
      { Integer: '2' },
      { Text: 'Bob' },
      { Text: 'active' },
    ],
  ]);

  const invalidAddColumnErr = unwrapErr(
    await executeSql(
      `ALTER TABLE ${renamedTable} ADD COLUMN required_note TEXT NOT NULL`,
      'add not null column without default'
    ),
    'add not null column without default'
  );
  assert.ok(
    invalidAddColumnErr.includes('Cannot add a NOT NULL column without a non-NULL default value')
  );
}

export const schemaFeatureTestCases = [
  { fullName: 'testConstraintEnforcement', displayName: 'constraints enforce not null unique primary key and defaults', fn: testConstraintEnforcement },
  { fullName: 'testPrimaryKeyOperations', displayName: 'primary key insert select update delete work', fn: testPrimaryKeyOperations },
  { fullName: 'testInsertFromSelect', displayName: 'insert into select copies query results into target table', fn: testInsertFromSelect },
  { fullName: 'testCheckConstraints', displayName: 'check constraints reject invalid insert and update', fn: testCheckConstraints },
  { fullName: 'testUpsertOperations', displayName: 'upsert supports do nothing and do update', fn: testUpsertOperations },
  { fullName: 'testAlterTableFeatures', displayName: 'alter table rename and add column work', fn: testAlterTableFeatures },
];