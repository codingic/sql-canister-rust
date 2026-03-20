import { schemaFeatureTestCases } from './schema-features.js';
import { selectFeatureTestCases } from './select-features.js';
import { subqueryFeatureTestCases } from './subquery-features.js';
import { compoundFeatureTestCases } from './compound-features.js';
import { joinFeatureTestCases } from './join-features.js';

export const sqlFeatureTestCases = [
  ...schemaFeatureTestCases,
  ...selectFeatureTestCases,
  ...subqueryFeatureTestCases,
  ...compoundFeatureTestCases,
  ...joinFeatureTestCases,
];