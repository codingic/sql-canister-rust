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
  { fullName: 'testAlterTableFeatures', displayName: 'alter table rename and add column work', fn: testAlterTableFeatures },
];