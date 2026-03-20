export const idlFactory = ({ IDL }) => {
  const ExecuteResult = IDL.Record({ 'message' : IDL.Text });
  const SqlValue = IDL.Variant({
    'Blob' : IDL.Vec(IDL.Nat8),
    'Null' : IDL.Null,
    'Text' : IDL.Text,
    'Float' : IDL.Float64,
    'Integer' : IDL.Int64,
  });
  const QueryResult = IDL.Record({
    'rows' : IDL.Vec(IDL.Vec(SqlValue)),
    'columns' : IDL.Vec(IDL.Text),
  });
  const BatchExecuteResult = IDL.Record({
    'changed_schema_or_data' : IDL.Bool,
    'statements_executed' : IDL.Nat32,
    'has_query_result' : IDL.Bool,
    'last_query_result' : QueryResult,
  });
  const DatabaseInfo = IDL.Record({ 'tables' : IDL.Vec(IDL.Text) });
  return IDL.Service({
    'execute' : IDL.Func(
        [IDL.Text],
        [IDL.Variant({ 'Ok' : ExecuteResult, 'Err' : IDL.Text })],
        [],
      ),
    'execute_batch' : IDL.Func(
        [IDL.Vec(IDL.Text)],
        [IDL.Variant({ 'Ok' : BatchExecuteResult, 'Err' : IDL.Text })],
        [],
      ),
    'info' : IDL.Func([], [DatabaseInfo], ['query']),
    'query' : IDL.Func(
        [IDL.Text],
        [IDL.Variant({ 'Ok' : QueryResult, 'Err' : IDL.Text })],
        ['query'],
      ),
  });
};
export const init = ({ IDL }) => { return []; };
