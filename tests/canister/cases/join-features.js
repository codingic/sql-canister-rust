import {
  assert,
  createTableName,
  assertExecuteOk,
  createTableSql,
  insertSql,
  assertQueryOk,
} from '../harness.js';

async function testMultiTableSelectFeatures() {
  const usersTable = createTableName('multi_users');
  const ordersTable = createTableName('multi_orders');
  const profilesTable = createTableName('multi_profiles');
  const userColumns = [
    { name: 'id', type: 'INTEGER' },
    { name: 'name', type: 'TEXT' },
    { name: 'team', type: 'TEXT' },
  ];
  const orderColumns = [
    { name: 'id', type: 'INTEGER' },
    { name: 'user_id', type: 'INTEGER' },
    { name: 'amount', type: 'REAL' },
  ];
  const profileColumns = [
    { name: 'id', type: 'INTEGER' },
    { name: 'nickname', type: 'TEXT' },
  ];

  await assertExecuteOk(createTableSql(usersTable, userColumns), 'create multi table users');
  await assertExecuteOk(createTableSql(ordersTable, orderColumns), 'create multi table orders');
  await assertExecuteOk(createTableSql(profilesTable, profileColumns), 'create multi table profiles');

  for (const row of [
    [1, 'Alice', 'red'],
    [2, 'Bob', 'blue'],
  ]) {
    await assertExecuteOk(insertSql(usersTable, userColumns, row), `insert multi table user ${row[0]}`);
  }

  for (const row of [
    [101, 1, 19.5],
    [102, 1, 42.25],
    [103, 2, 15.75],
  ]) {
    await assertExecuteOk(insertSql(ordersTable, orderColumns, row), `insert multi table order ${row[0]}`);
  }

  await assertExecuteOk(
    insertSql(profilesTable, profileColumns, [1, 'ally']),
    'insert multi table profile 1'
  );

  const aliasResult = await assertQueryOk(
    `SELECT u.name AS user_name, o.amount FROM ${usersTable} AS u, ${ordersTable} AS o WHERE u.id = o.user_id ORDER BY o.amount DESC LIMIT 2`,
    'query multiple tables with aliases'
  );
  assert.deepEqual(aliasResult.columns, ['user_name', 'amount']);
  assert.deepEqual(aliasResult.rows, [
    [
      { Text: 'Alice' },
      { Float: 42.25 },
    ],
    [
      { Text: 'Alice' },
      { Float: 19.5 },
    ],
  ]);

  const joinResult = await assertQueryOk(
    `SELECT u.name AS user_name, o.amount FROM ${usersTable} AS u JOIN ${ordersTable} AS o ON u.id = o.user_id WHERE o.amount >= 19 ORDER BY o.amount DESC`,
    'query explicit join with on'
  );
  assert.deepEqual(joinResult.columns, ['user_name', 'amount']);
  assert.deepEqual(joinResult.rows, [
    [
      { Text: 'Alice' },
      { Float: 42.25 },
    ],
    [
      { Text: 'Alice' },
      { Float: 19.5 },
    ],
  ]);

  const starResult = await assertQueryOk(
    `SELECT ${usersTable}.*, ${ordersTable}.amount FROM ${usersTable}, ${ordersTable} WHERE ${usersTable}.id = ${ordersTable}.user_id AND ${ordersTable}.id = 103`,
    'query multiple tables with table star'
  );
  assert.deepEqual(starResult.columns, ['id', 'name', 'team', 'amount']);
  assert.deepEqual(starResult.rows, [
    [
      { Integer: '2' },
      { Text: 'Bob' },
      { Text: 'blue' },
      { Float: 15.75 },
    ],
  ]);

  const crossJoinCount = await assertQueryOk(
    `SELECT COUNT(*) AS pair_count FROM ${usersTable} CROSS JOIN ${ordersTable}`,
    'query cross join count'
  );
  assert.deepEqual(crossJoinCount.columns, ['pair_count']);
  assert.deepEqual(crossJoinCount.rows, [
    [{ Integer: '6' }],
  ]);

  const leftJoinResult = await assertQueryOk(
    `SELECT u.name, p.nickname FROM ${usersTable} AS u LEFT JOIN ${profilesTable} AS p ON u.id = p.id ORDER BY u.id`,
    'query left join with on'
  );
  assert.deepEqual(leftJoinResult.columns, ['name', 'nickname']);
  assert.deepEqual(leftJoinResult.rows, [
    [
      { Text: 'Alice' },
      { Text: 'ally' },
    ],
    [
      { Text: 'Bob' },
      { Null: null },
    ],
  ]);

  const usingJoinResult = await assertQueryOk(
    `SELECT ${usersTable}.id, ${profilesTable}.nickname FROM ${usersTable} LEFT JOIN ${profilesTable} USING (id) ORDER BY ${usersTable}.id`,
    'query left join with using'
  );
  assert.deepEqual(usingJoinResult.columns, ['id', 'nickname']);
  assert.deepEqual(usingJoinResult.rows, [
    [
      { Integer: '1' },
      { Text: 'ally' },
    ],
    [
      { Integer: '2' },
      { Null: null },
    ],
  ]);
}

export const joinFeatureTestCases = [
  { fullName: 'testMultiTableSelectFeatures', displayName: 'multi table select supports aliases left join using and table star', fn: testMultiTableSelectFeatures },
];