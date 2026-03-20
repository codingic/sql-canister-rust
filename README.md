# SQL Database Canister

这个项目把一个纯 Rust 的 SQL 引擎封装成了 Internet Computer canister，并提供了一个最小可用的前端数据库浏览器。

数据库在运行期保存在内存里，但 canister 升级前会自动把数据快照写入 stable memory，升级后再恢复，所以升级不会丢数据。

## 当前功能

后端能力：

- 支持 `CREATE TABLE`、`INSERT`、`UPDATE`、`DELETE`、`DROP`
- 支持 `SELECT` 查询并返回结构化结果
- 支持 `BEGIN`、`COMMIT`、`ROLLBACK` 基础事务
- 支持 `execute_batch` 一次执行多条 SQL，减少大 `.sql` 文件导入时的 canister 往返次数
- 支持 canister 升级后的数据持久化恢复
- 支持中文表名、中文列名、中文字符串内容

前端能力：

- 页面加载时自动调用 `info()` 显示所有表
- 点击表名后自动查询并展示表数据
- 结果区支持分页浏览
- 支持手动输入 SQL 执行
- 支持选择 `.sql` 文件并按顺序拆分 SQL 语句后批量执行
- 支持选择 `.xlsx` 文件并自动转换成建表加导入 SQL
- `.xlsx` 导入会按批量 `INSERT` 生成 SQL，并自动包裹事务，避免逐行导入过慢

测试覆盖：

- 三张表建表和写入校验
- 每张表 10 列的宽表校验
- 三张主表每张表插入 100 行数据校验
- 中文表、中文列、中文内容严格兼容校验
- 单表插入 100 条中文数据校验
- 升级后中文数据和普通数据保持不丢失
- 事务提交和回滚校验
- 非法语句和不支持语句的报错校验
- `.xlsx` 导入转换 SQL 校验
- 批量执行接口对混合语句、事务和最后一次查询结果的校验

## Canister 接口

- `execute(sql: text)`：执行单条写操作 SQL
- `execute_batch(statements: vec text)`：在一次 update 调用里顺序执行多条 SQL，并返回最后一次查询结果
- `query(sql: text)`：执行 `SELECT` 查询
- `info()`：返回当前数据库里的全部表名

约束：

- `query` 只接受 `SELECT`
- `execute` 不接受 `SELECT`
- `execute_batch` 接收已经拆分好的 SQL 语句数组，按顺序逐条执行
- `.xlsx` 导入本质上也是前端先转换成 SQL，再通过 `execute_batch` 统一提交到后端执行

## 返回结构

`query()` 返回：

- `columns`：列名数组
- `rows`：二维数组

`execute_batch()` 返回：

- `statements_executed`：本次批量执行的语句数
- `changed_schema_or_data`：是否发生了表结构或数据变更
- `has_query_result`：批量执行里是否包含查询结果
- `last_query_result`：最后一次 `SELECT` 的结果；如果没有查询则为空结果集

单元格值使用 `SqlValue` 变体表示，支持：

- `Null`
- `Integer`
- `Float`
- `Text`
- `Blob`

## 本地开发

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

5. 生产构建

```bash
npm run build
```

## 命令行调用示例

```bash
dfx canister call backend execute '("CREATE TABLE 用户表 (编号 INTEGER, 姓名 TEXT, 城市 TEXT)")'
dfx canister call backend execute '("INSERT INTO 用户表 (编号, 姓名, 城市) VALUES (1, ''张三'', ''北京'')")'
dfx canister call backend query '("SELECT 编号, 姓名, 城市 FROM 用户表 ORDER BY 编号")' --output json
dfx canister call backend execute_batch '(vec {"BEGIN TRANSACTION"; "INSERT INTO 用户表 (编号, 姓名, 城市) VALUES (2, ''李四'', ''杭州'')"; "COMMIT"; "SELECT 编号, 姓名, 城市 FROM 用户表 ORDER BY 编号"})' --output json
dfx canister call backend info --output json
```

## 测试

运行完整集成测试：

```bash
npm run test:canister
```

运行文件导入转换测试：

```bash
npm run test:file-import
```

这套 canister 测试会自动：

- 重置本地副本环境
- 部署 `backend` canister
- 执行建表、插入、查询、更新、删除、事务、批量执行、升级恢复校验
- 检查三表宽表场景、100 条中文数据场景和升级持久化场景

文件导入测试会自动：

- 生成一个测试用 `.xlsx` 工作簿
- 验证工作表会被转换成 `BEGIN`、`CREATE TABLE`、批量 `INSERT`、`COMMIT` SQL
- 验证中文表名、中文列名和数值类型推断

## 限制说明

- 当前实现更适合单表 CRUD、基础查询和验证性场景，不是完整 SQLite 兼容实现
- 前端已支持 `.sql` 文件选择执行，但“拖拽上传 / 独立文件执行进度 / 失败定位”还未落地
- `.xlsx` 当前支持导入首行表头加数据区，并转换为单工作表或多工作表 SQL；还没有做复杂格式、公式结果校验和大文件分片上传
