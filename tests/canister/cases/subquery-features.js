import {
  assert,
  createTableName,
  assertExecuteOk,
  createTableSql,
  insertSql,
  assertQueryOk,
  unwrapErr,
  querySql,
} from '../harness.js';

async function testSubqueryFeatures() {
  const tableName = createTableName('subquery_select');
  const columns = [
    { name: 'id', type: 'INTEGER' },
    { name: 'category', type: 'TEXT' },
    { name: 'score', type: 'REAL' },
    { name: 'note', type: 'TEXT' },
  ];

  await assertExecuteOk(
    createTableSql(tableName, columns),
    'create subquery select table'
  );

  for (const row of [
    [1, 'alpha', 10.0, null],
    [2, 'alpha', 15.0, 'eligible'],
    [3, 'beta', 22.5, 'beta-only'],
    [4, 'alpha', 25.0, 'priority'],
    [5, 'gamma', 30.0, null],
    [6, 'alpha', 28.0, 'latest'],
  ]) {
    await assertExecuteOk(
      insertSql(tableName, columns, row),
      `insert subquery row ${row[0]}`
    );
  }

  const subqueryResult = await assertQueryOk(
    `SELECT id FROM ${tableName} WHERE id IN (SELECT id FROM ${tableName} WHERE category = 'alpha' AND note IS NOT NULL) ORDER BY id`,
    'query in subquery filter'
  );
  assert.deepEqual(subqueryResult.columns, ['id']);
  assert.deepEqual(subqueryResult.rows, [
    [{ Integer: '2' }],
    [{ Integer: '4' }],
    [{ Integer: '6' }],
  ]);

  const existsResult = await assertQueryOk(
    `SELECT id FROM ${tableName} WHERE EXISTS (SELECT id FROM ${tableName} WHERE category = 'gamma') ORDER BY id LIMIT 2`,
    'query exists subquery filter'
  );
  assert.deepEqual(existsResult.columns, ['id']);
  assert.deepEqual(existsResult.rows, [
    [{ Integer: '1' }],
    [{ Integer: '2' }],
  ]);

  const notExistsResult = await assertQueryOk(
    `SELECT id FROM ${tableName} WHERE NOT EXISTS (SELECT id FROM ${tableName} WHERE category = 'missing') ORDER BY id DESC LIMIT 1`,
    'query not exists subquery filter'
  );
  assert.deepEqual(notExistsResult.columns, ['id']);
  assert.deepEqual(notExistsResult.rows, [
    [{ Integer: '6' }],
  ]);

  const scalarProjectionResult = await assertQueryOk(
    `SELECT (SELECT category FROM ${tableName} WHERE id = 3) AS picked_category`,
    'query scalar subquery projection'
  );
  assert.deepEqual(scalarProjectionResult.columns, ['picked_category']);
  assert.deepEqual(scalarProjectionResult.rows, [
    [{ Text: 'beta' }],
  ]);

  const scalarComparisonResult = await assertQueryOk(
    `SELECT id FROM ${tableName} WHERE score > (SELECT 20.0) ORDER BY id`,
    'query scalar subquery comparison'
  );
  assert.deepEqual(scalarComparisonResult.columns, ['id']);
  assert.deepEqual(scalarComparisonResult.rows, [
    [{ Integer: '3' }],
    [{ Integer: '4' }],
    [{ Integer: '5' }],
    [{ Integer: '6' }],
  ]);

  const invalidSubqueryErr = unwrapErr(
    await querySql(
      `SELECT id FROM ${tableName} WHERE id IN (SELECT id, score FROM ${tableName})`,
      'query in subquery with multiple columns'
    ),
    'query in subquery with multiple columns'
  );
  assert.ok(invalidSubqueryErr.includes('subquery for IN must return exactly one column'));

  const invalidScalarColumnsErr = unwrapErr(
    await querySql(
      `SELECT id FROM ${tableName} WHERE id = (SELECT id, score FROM ${tableName} WHERE id = 1)`,
      'query scalar subquery with multiple columns'
    ),
    'query scalar subquery with multiple columns'
  );
  assert.ok(invalidScalarColumnsErr.includes('scalar subquery must return exactly one column'));

  const invalidScalarRowsErr = unwrapErr(
    await querySql(
      `SELECT id FROM ${tableName} WHERE id = (SELECT id FROM ${tableName} WHERE category = 'alpha')`,
      'query scalar subquery with multiple rows'
    ),
    'query scalar subquery with multiple rows'
  );
  assert.ok(invalidScalarRowsErr.includes('scalar subquery must return at most one row'));
}

export const subqueryFeatureTestCases = [
  { fullName: 'testSubqueryFeatures', displayName: 'in exists not exists and scalar subqueries work', fn: testSubqueryFeatures },
];