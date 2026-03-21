# SQL Database Canister

这个项目把一个纯 Rust 的 SQL 引擎封装成了 Internet Computer canister，并提供了一个最小可用的前端数据库浏览器。

数据库在运行期保存在内存里，但 canister 升级前会自动把数据快照写入 stable memory，升级后再恢复，所以升级不会丢数据。

## 快速概览

- 后端提供一个可升级持久化的 SQL canister，支持基础 DDL、DML、事务、批量执行、中文兼容，以及一部分接近 SQLite 的查询语法
- 前端提供表浏览、表名筛选、结果分页/跳页、手动 SQL 执行、最近 SQL 历史、`.sql` / `.xlsx` 导入和 `.sql` / `.xlsx` 导出
- 测试覆盖 canister 集成链路、文件导入导出转换、升级持久化、约束、聚合、多表查询、compound query 与基础子查询
- 详细支持边界请直接看后面的“SQL 功能矩阵”和“前端与接口矩阵”

## 近期功能说明

这一轮主要补齐了单表写入与约束链路里最常用的一批能力，并同步补上了自动化测试。

- `INSERT INTO ... SELECT`：现在可以把普通 `SELECT` 或 compound query 的结果直接插入目标表，支持列映射和列数不匹配报错
- `CHECK` 约束：支持列级 `CHECK (...)` 表达式，在 `INSERT`、`UPDATE`、升级恢复和运行时状态重建时统一校验；当前不支持子查询和聚合函数
- `UPSERT`：支持单个 `ON CONFLICT` 子句、`DO NOTHING`、`DO UPDATE SET ... [WHERE ...]`，支持在 `DO UPDATE` 中读取目标表当前值和 `excluded` 新值；当前仅支持单列冲突目标
- 主键快速路径：单表 `SELECT/UPDATE/DELETE ... WHERE 主键 = ...` 会优先走主键定位，减少全表扫描
- 前端效率增强：新增表名筛选、页码跳转、结果一键复制为 TSV、Cmd/Ctrl + Enter 快捷执行、最近 SQL 历史复用

对应验证已经覆盖：Rust 后端全量测试、canister 集成测试、文件导入导出测试和前端构建检查均已通过。

## 快速开始

1. 安装依赖

```bash
npm install
```

2. 启动本地副本

```bash
dfx start --clean --background
```

3. 部署 backend canister

```bash
dfx deploy backend
```

4. 启动前端开发服务器

```bash
npm run dev
```

## 能力概览

后端能力：

- 基础 DDL / DML：`CREATE TABLE`、`ALTER TABLE` 基础子集、`INSERT`、`UPDATE`、`DELETE`、`DROP`
- 查询能力：`SELECT`、过滤表达式、排序分页、聚合分组、compound query、多表查询与基础 `JOIN`
- 子查询：支持基础非相关 `IN (SELECT ...)`、`EXISTS`、`NOT EXISTS` 与标量子查询
- 事务与批量执行：支持 `BEGIN`、`COMMIT`、`ROLLBACK` 与 `execute_batch`
- 数据兼容：支持中文表名、中文列名、中文字符串内容
- 升级持久化：canister 升级后自动恢复 stable memory 快照

前端能力：

- 表浏览：自动加载表列表，点击表名即可查看数据
- 结果查看：支持宽表横向滚动、分页浏览、页码跳转与结果复制
- 手动操作：支持直接输入 SQL 执行、Cmd/Ctrl + Enter 快捷执行与最近 SQL 历史复用
- 文件导入：支持 `.sql` 和 `.xlsx` 导入，`.xlsx` 会转换为批量 `INSERT` 并自动包裹事务
- 文件导出：支持把当前查询结果导出为 `.sql` 或 `.xlsx`

## SQL 功能矩阵

下面按 SQL 语法类别列出当前状态，方便直接判断某条语句是否可用。

### 1. DDL

| 语法类别 | 语法/能力 | 状态 | 说明 |
| --- | --- | --- | --- |
| 建表 | `CREATE TABLE table (...)` | 已支持 | 支持基础列定义与类型声明 |
| 删表 | `DROP TABLE table` | 已支持 | 支持直接删除表 |
| 改表 | `ALTER TABLE ... RENAME TO ...` | 已支持 | 支持重命名表 |
| 改表 | `ALTER TABLE ... RENAME COLUMN ... TO ...` | 已支持 | 支持重命名列 |
| 改表 | `ALTER TABLE ... ADD COLUMN ...` | 已支持 | 支持追加列，支持默认值回填 |
| 改表 | `ALTER TABLE ... DROP COLUMN ...` | 未支持 | AST 中有定义，但当前执行层未实现 |
| 索引 | `CREATE INDEX` / `DROP INDEX` | 未支持 | 当前无索引与查询规划实现 |
| 视图 | `CREATE VIEW` / `DROP VIEW` | 未支持 | 当前未实现 |
| 触发器 | `CREATE TRIGGER` / `DROP TRIGGER` | 未支持 | 当前未实现 |

### 2. DML

| 语法类别 | 语法/能力 | 状态 | 说明 |
| --- | --- | --- | --- |
| 插入 | `INSERT INTO ... VALUES (...)` | 已支持 | 支持显式列列表与多类型值 |
| 更新 | `UPDATE ... SET ... WHERE ...` | 已支持 | 支持条件更新 |
| 删除 | `DELETE FROM ... WHERE ...` | 已支持 | 支持条件删除 |
| 批量执行 | `execute_batch([sql1, sql2, ...])` | 已支持 | 一次 update 调用里顺序执行多条 SQL |
| 插入来源查询 | `INSERT INTO ... SELECT ...` | 已支持 | 支持把 `SELECT` 或 compound query 结果插入目标表 |
| UPSERT | `INSERT ... ON CONFLICT ...` | 部分支持 | 支持单个 `ON CONFLICT` 子句、`DO NOTHING`、`DO UPDATE SET ... [WHERE ...]`；当前仅支持单列冲突目标 |

### 3. 约束与列定义

| 语法类别 | 语法/能力 | 状态 | 说明 |
| --- | --- | --- | --- |
| 非空约束 | `NOT NULL` | 已支持 | `INSERT` / `UPDATE` 时强制校验 |
| 主键约束 | `PRIMARY KEY` | 已支持 | 当前按唯一且非空语义校验 |
| 唯一约束 | `UNIQUE` | 已支持 | `INSERT` / `UPDATE` 时强制校验 |
| 默认值 | `DEFAULT ...` | 已支持 | 建表与 `ALTER TABLE ADD COLUMN` 场景生效 |
| 自增主键 | `PRIMARY KEY AUTOINCREMENT` | 部分支持 | 语法可解析，但没有完整 SQLite 自增语义 |
| 检查约束 | `CHECK (...)` | 已支持 | 支持列级 `CHECK` 表达式；当前不支持子查询和聚合函数 |
| 外键 | `FOREIGN KEY ... REFERENCES ...` | 未支持 | 当前未实现 |

### 4. 单表查询

| 语法类别 | 语法/能力 | 状态 | 说明 |
| --- | --- | --- | --- |
| 基础查询 | `SELECT ... FROM table` | 已支持 | 返回结构化列与行 |
| 常量查询 | `SELECT 1`, `SELECT 1 / 2` | 已支持 | 支持无 `FROM` 的表达式查询 |
| 去重 | `DISTINCT` | 已支持 | 支持和排序、分页组合 |
| 排序 | `ORDER BY` | 已支持 | 支持普通查询与 compound 最外层排序 |
| 分页 | `LIMIT` / `OFFSET` | 已支持 | 支持普通查询与 compound 最外层分页 |
| 别名 | `AS` | 已支持 | 支持列表达式别名与表别名 |
| 全列展开 | `*` / `table.*` | 已支持 | 支持单表和多表场景 |
| 空结果列保留 | `SELECT ... WHERE 1=0` | 已支持 | 空结果集仍返回列名 |

### 5. 过滤与表达式

| 语法类别 | 语法/能力 | 状态 | 说明 |
| --- | --- | --- | --- |
| 比较运算 | `=`, `!=`, `<`, `<=`, `>`, `>=` | 已支持 | 可用于 `WHERE`、`HAVING`、表达式求值 |
| 逻辑运算 | `AND`, `OR`, `NOT` | 已支持 | 支持布尔条件组合 |
| 算术运算 | `+`, `-`, `*`, `/`, `%` | 已支持 | `/` 当前返回浮点除法结果 |
| 字符串拼接 | `\|\|` | 已支持 | 支持文本拼接 |
| 区间判断 | `BETWEEN ... AND ...` | 已支持 | 支持 `NOT BETWEEN` |
| 集合判断 | `IN (...)` | 已支持 | 支持显式列表和子查询来源 |
| 空值判断 | `IS NULL`, `IS NOT NULL` | 已支持 | 支持等价语法变体 |
| 模式匹配 | `LIKE` | 已支持 | 支持基础模式匹配 |
| 模式匹配 | `GLOB` | 部分支持 | 语法按 `LIKE` 路径处理，不是完整 SQLite `GLOB` 语义 |
| 一元运算 | `-expr`, `~expr`, `NOT expr` | 已支持 | 支持数值取负、位取反、逻辑非 |

### 6. 聚合与分组

| 语法类别 | 语法/能力 | 状态 | 说明 |
| --- | --- | --- | --- |
| 分组 | `GROUP BY` | 已支持 | 支持和聚合函数组合 |
| 分组过滤 | `HAVING` | 已支持 | 支持聚合条件和普通条件 |
| 聚合函数 | `COUNT`, `SUM`, `AVG`, `MIN`, `MAX` | 已支持 | 当前支持基础聚合 |
| 非聚合函数 | `UPPER`, `LOWER`, `LENGTH`, `ABS`, `COALESCE`, `IFNULL`, `TYPEOF` | 已支持 | 当前实现的常用标量函数 |
| 更多 SQLite 内建函数 | `ROUND`, `SUBSTR`, `DATE` 等 | 未支持 | 当前未系统实现 |

### 7. 多表查询与连接

| 语法类别 | 语法/能力 | 状态 | 说明 |
| --- | --- | --- | --- |
| 多表 FROM | `FROM a, b` | 已支持 | 当前按左深组合构建结果行 |
| 内连接 | `JOIN ... ON ...` | 已支持 | 支持 `JOIN` 与 `INNER JOIN` |
| 左连接 | `LEFT JOIN ... ON ...` | 已支持 | 支持 `LEFT OUTER JOIN` |
| USING 连接 | `JOIN ... USING (...)` | 已支持 | 支持共享列匹配 |
| 笛卡尔积 | `CROSS JOIN` | 已支持 | 支持基础语义 |
| 右连接 | `RIGHT JOIN` | 未支持 | parser 明确拒绝 |
| 全连接 | `FULL JOIN` | 未支持 | parser 明确拒绝 |
| 自然连接 | `NATURAL JOIN` | 未支持 | parser 明确拒绝 |
| 连接子查询 | `FROM (SELECT ...) t` | 未支持 | 当前 `FROM` 仅支持真实表名 |

### 8. Compound Query

| 语法类别 | 语法/能力 | 状态 | 说明 |
| --- | --- | --- | --- |
| 并集去重 | `UNION` | 已支持 | 支持最外层排序和分页 |
| 并集保留重复 | `UNION ALL` | 已支持 | 支持最外层排序和分页 |
| 交集 | `INTERSECT` | 已支持 | 当前仅 distinct 语义 |
| 差集 | `EXCEPT` | 已支持 | 当前仅 distinct 语义 |
| 外层排序分页 | `(... UNION ...) ORDER BY ... LIMIT ... OFFSET ...` | 已支持 | 已覆盖测试 |
| `INTERSECT ALL` | 未支持 | parser 明确报错 |
| `EXCEPT ALL` | 未支持 | parser 明确报错 |

### 9. 子查询

| 语法类别 | 语法/能力 | 状态 | 说明 |
| --- | --- | --- | --- |
| 集合子查询 | `expr IN (SELECT ...)` | 已支持 | 子查询必须返回一列 |
| 存在性子查询 | `EXISTS (SELECT ...)` | 已支持 | 支持 `NOT EXISTS` |
| 标量子查询 | `expr = (SELECT ...)`、`SELECT (SELECT ...)` | 已支持 | 子查询必须最多一行且一列 |
| 非相关子查询 | 独立子查询 | 已支持 | 当前执行时会先物化子查询结果 |
| 相关子查询 | 引用外层行的子查询 | 未支持 | 当前未实现 |
| FROM 子查询 | `FROM (SELECT ...)` | 未支持 | 当前未实现 |

### 10. 事务与执行边界

| 语法类别 | 语法/能力 | 状态 | 说明 |
| --- | --- | --- | --- |
| 开启事务 | `BEGIN`, `BEGIN TRANSACTION` | 已支持 | 当前支持基础事务 |
| 提交事务 | `COMMIT` | 已支持 | 已覆盖测试 |
| 回滚事务 | `ROLLBACK` | 已支持 | 已覆盖测试 |
| 查询接口限制 | `query(sql)` 只接受 `SELECT` | 已支持 | 非查询语句会直接报错 |
| 执行接口限制 | `execute(sql)` 不接受 `SELECT` | 已支持 | `SELECT` 需走 `query` |

### 11. 其他 SQLite 语法

| 语法类别 | 语法/能力 | 状态 | 说明 |
| --- | --- | --- | --- |
| `PRAGMA` | `PRAGMA ...` | 未支持 | 会返回 unsupported execute statement |
| `EXPLAIN` | `EXPLAIN ...` | 未支持 | 当前未实现 |
| `VACUUM` | `VACUUM` | 未支持 | 当前未实现 |
| `ANALYZE` | `ANALYZE` | 未支持 | 当前未实现 |
| `ATTACH` / `DETACH` | 多数据库管理 | 未支持 | 当前未实现 |

## 前端与接口矩阵

### 1. 前端功能

| 功能类别 | 功能/行为 | 状态 | 说明 |
| --- | --- | --- | --- |
| 表浏览 | 页面初始化后自动加载表列表 | 已支持 | 页面启动时会调用 `info()` |
| 表浏览 | 表名筛选 | 已支持 | 可按关键词快速过滤表列表 |
| 表浏览 | 点击表名查看数据 | 已支持 | 当前会自动生成查询并渲染结果 |
| 结果展示 | 表格横向滚动与宽表展示 | 已支持 | 已针对宽表场景做容器与表格宽度处理 |
| 结果展示 | 结果分页浏览 | 已支持 | 支持页大小切换、上一页、下一页 |
| 结果展示 | 页码跳转 | 已支持 | 支持直接跳转到指定页 |
| 结果展示 | 复制当前结果 | 已支持 | 支持一键复制为 TSV 文本 |
| 手动执行 | 输入 SQL 后执行 | 已支持 | 查询和写入都可从页面触发 |
| 手动执行 | Cmd/Ctrl + Enter 快捷执行 | 已支持 | 便于减少鼠标操作 |
| 手动执行 | 最近 SQL 历史 | 已支持 | 会保存最近成功执行的 SQL 便于重复使用 |
| 导入 | `.sql` 文件导入 | 已支持 | 前端拆分语句后调用 `execute_batch` |
| 导入 | `.xlsx` 文件导入 | 已支持 | 自动转换为建表与批量插入 SQL |
| 导入 | 大文件批量插入优化 | 已支持 | `.xlsx` 导入会生成批量 `INSERT` 并包裹事务 |
| 导出 | 当前结果导出 `.sql` | 已支持 | 导出为建表加批量插入 SQL |
| 导出 | 当前结果导出 `.xlsx` | 已支持 | 导出为单工作表 Excel |
| 状态反馈 | 成功/失败状态提示 | 已支持 | 页面会显示状态消息 |
| 拖拽上传 | 拖拽 `.sql` / `.xlsx` 文件 | 未支持 | 当前仅支持文件选择器 |
| 进度展示 | 导入进度百分比 | 未支持 | 当前没有逐阶段进度条 |
| 失败定位 | 精确定位到第几条 SQL 失败 | 未支持 | 当前主要返回后端报错文本 |

### 2. Canister 接口能力

| 接口 | 能力 | 状态 | 说明 |
| --- | --- | --- | --- |
| `info()` | 返回全部表名 | 已支持 | 前端表列表依赖该接口 |
| `query(sql)` | 执行查询语句 | 已支持 | 当前只允许 `SELECT` / compound `SELECT` |
| `execute(sql)` | 执行单条写语句 | 已支持 | 不允许 `SELECT` |
| `execute_batch(statements)` | 顺序执行多条语句 | 已支持 | 可返回最后一次查询结果 |
| `execute_batch(statements)` | 混合事务与查询 | 已支持 | 支持 `BEGIN`/`COMMIT`/写入/最后查询组合 |
| `query(sql)` | 非查询语句报错 | 已支持 | 会直接返回接口级错误 |
| `execute(sql)` | 查询语句报错 | 已支持 | 会提示改用 `query` |
| `execute_batch(statements)` | 自动 SQL 拆分 | 未支持 | 当前要求调用方先拆分好语句数组 |

### 3. 返回结果与类型

| 类别 | 能力 | 状态 | 说明 |
| --- | --- | --- | --- |
| 查询结果 | `columns` 列名返回 | 已支持 | 空结果集也保留列名 |
| 查询结果 | `rows` 二维数组返回 | 已支持 | 前端表格直接消费 |
| 数值类型 | `Integer` / `Float` | 已支持 | 区分整数与浮点 |
| 文本类型 | `Text` | 已支持 | 支持中文内容 |
| 空值类型 | `Null` | 已支持 | 前端可显示为 `NULL` |
| 二进制类型 | `Blob` | 已支持 | 查询结果类型已定义，导出时支持转 SQL/XLSX 表达 |
| 批量结果 | `last_query_result` | 已支持 | 便于导入后立即拿到最后一次查询结果 |

## 测试

测试覆盖：

- 三张表建表和写入校验
- 每张表 10 列的宽表校验
- 三张主表每张表插入 100 行数据校验
- 约束校验：`NOT NULL`、`PRIMARY KEY`、`UNIQUE`、`DEFAULT`
- 表结构变更校验：`ALTER TABLE` 重命名表、重命名列、追加带默认值的新列
- 查询语义校验：过滤、分页、聚合、分组、多表查询、显式 `JOIN`
- compound query 校验：`UNION`、`UNION ALL`、`INTERSECT`、`EXCEPT` 以及外层排序分页
- 基础子查询校验：`IN (SELECT ...)`、`EXISTS`、`NOT EXISTS`、标量子查询
- 中文表、中文列、中文内容严格兼容校验
- 单表插入 100 条中文数据校验
- 升级后中文数据和普通数据保持不丢失
- 事务提交和回滚校验
- 非法语句和不支持语句的报错校验
- `.xlsx` 导入转换 SQL 校验
- 查询结果导出 `.sql` / `.xlsx` 转换校验
- 批量执行接口对混合语句、事务和最后一次查询结果的校验

常用测试命令：

```bash
npm run test:canister
node tests/backend.canister.test.js --list
node tests/backend.canister.test.js testConstraintEnforcement
node tests/backend.canister.test.js testAdvancedSelectFeatures testGroupedSelectFeatures testMultiTableSelectFeatures
npm run test:file-import
```

测试目录结构：

- [tests/backend.canister.test.js](tests/backend.canister.test.js)：canister 集成测试入口，支持 `--list` 和按功能全名筛选
- [tests/canister/harness.js](tests/canister/harness.js)：dfx 调用、SQL 辅助函数、测试筛选与公共断言
- [tests/canister/cases/basic.js](tests/canister/cases/basic.js)：基础 CRUD 与类型化结果校验
- [tests/canister/cases/sql-features.js](tests/canister/cases/sql-features.js)：SQL 功能测试聚合入口
- [tests/canister/cases/schema-features.js](tests/canister/cases/schema-features.js)：约束与 `ALTER TABLE` 测试
- [tests/canister/cases/select-features.js](tests/canister/cases/select-features.js)：过滤、排序、分页、聚合、分组测试
- [tests/canister/cases/subquery-features.js](tests/canister/cases/subquery-features.js)：基础子查询测试
- [tests/canister/cases/compound-features.js](tests/canister/cases/compound-features.js)：compound query 与错误处理测试
- [tests/canister/cases/join-features.js](tests/canister/cases/join-features.js)：多表查询与连接测试
- [tests/canister/cases/lifecycle.js](tests/canister/cases/lifecycle.js)：事务、升级持久化、错误透出与中文兼容测试

说明：

- canister 测试会自动重置本地副本环境、部署 `backend` canister，并执行建表、写入、查询、事务、批量执行与升级恢复校验
- 文件导入导出测试会验证 `.xlsx` 转 SQL、中文表头与类型推断，以及查询结果导出 `.sql` / `.xlsx`

## Canister 调用摘要

前面的“SQL 功能矩阵”和“前端与接口矩阵”负责说明完整支持边界；这里仅保留日常调用最常用的信息。

接口：

- `info()`：返回当前数据库里的全部表名
- `query(sql: text)`：执行查询语句，当前只接受 `SELECT` 或 compound `SELECT`
- `execute(sql: text)`：执行单条写语句，不接受 `SELECT`
- `execute_batch(statements: vec text)`：顺序执行多条已拆分 SQL，并返回最后一次查询结果

结果结构：

- `query()` 返回 `columns` 与 `rows`
- `execute_batch()` 返回 `statements_executed`、`changed_schema_or_data`、`has_query_result`、`last_query_result`
- 单元格值使用 `SqlValue` 变体表示：`Null`、`Integer`、`Float`、`Text`、`Blob`

调用约束：

- `query` 只接受查询语句
- `execute` 只接受写语句
- `execute_batch` 需要调用方先拆分好 SQL 语句数组
- `.xlsx` 导入本质上是前端先转成 SQL，再通过 `execute_batch` 提交到后端执行

## 命令行调用示例

```bash
dfx canister call backend execute '("CREATE TABLE 用户表 (编号 INTEGER, 姓名 TEXT, 城市 TEXT)")'
dfx canister call backend execute '("INSERT INTO 用户表 (编号, 姓名, 城市) VALUES (1, ''张三'', ''北京'')")'
dfx canister call backend query '("SELECT 编号, 姓名, 城市 FROM 用户表 ORDER BY 编号")' --output json
dfx canister call backend execute_batch '(vec {"BEGIN TRANSACTION"; "INSERT INTO 用户表 (编号, 姓名, 城市) VALUES (2, ''李四'', ''杭州'')"; "COMMIT"; "SELECT 编号, 姓名, 城市 FROM 用户表 ORDER BY 编号"})' --output json
dfx canister call backend info --output json
```

## 更多开发命令

“快速开始”已经覆盖最短启动路径；这里仅保留补充性的构建命令。

```bash
npm run build
```

## 限制说明

- 当前实现更适合单表 CRUD、基础查询和验证性场景，不是完整 SQLite 兼容实现
- 当前只支持基础非相关子查询；暂不支持相关子查询和 `FROM (SELECT ...)` 形式的子查询
- 当前只支持 `JOIN`、`INNER JOIN`、`LEFT JOIN`、`LEFT OUTER JOIN`、`CROSS JOIN`；暂不支持 `RIGHT JOIN`、`FULL JOIN`、`NATURAL JOIN`
- 前端已支持 `.sql` 文件选择执行，但“拖拽上传 / 独立文件执行进度 / 失败定位”还未落地
- `.xlsx` 当前支持导入首行表头加数据区，并转换为单工作表或多工作表 SQL；还没有做复杂格式、公式结果校验和大文件分片上传
