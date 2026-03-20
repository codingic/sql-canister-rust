import type { Principal } from '@dfinity/principal';
import type { ActorMethod } from '@dfinity/agent';
import type { IDL } from '@dfinity/candid';

export interface BatchExecuteResult {
  'changed_schema_or_data' : boolean,
  'statements_executed' : number,
  'has_query_result' : boolean,
  'last_query_result' : QueryResult,
}
export interface DatabaseInfo { 'tables' : Array<string> }
export interface ExecuteResult { 'message' : string }
export interface QueryResult {
  'rows' : Array<Array<SqlValue>>,
  'columns' : Array<string>,
}
export type SqlValue = { 'Blob' : Uint8Array | number[] } |
  { 'Null' : null } |
  { 'Text' : string } |
  { 'Float' : number } |
  { 'Integer' : bigint };
export interface _SERVICE {
  'execute' : ActorMethod<
    [string],
    { 'Ok' : ExecuteResult } |
      { 'Err' : string }
  >,
  'execute_batch' : ActorMethod<
    [Array<string>],
    { 'Ok' : BatchExecuteResult } |
      { 'Err' : string }
  >,
  'info' : ActorMethod<[], DatabaseInfo>,
  'query' : ActorMethod<[string], { 'Ok' : QueryResult } | { 'Err' : string }>,
}
export declare const idlFactory: IDL.InterfaceFactory;
export declare const init: (args: { IDL: typeof IDL }) => IDL.Type[];
