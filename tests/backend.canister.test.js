import { basicTestCases } from './canister/cases/basic.js';
import { sqlFeatureTestCases } from './canister/cases/sql-features.js';
import { lifecycleTestCases } from './canister/cases/lifecycle.js';
import {
  resetLocalReplica,
  runDfx,
  runCase,
  resolveSelectedTestCases,
  parseRequestedTestNames,
} from './canister/harness.js';

const testCases = [
  ...basicTestCases,
  ...sqlFeatureTestCases,
  ...lifecycleTestCases,
];

async function main() {
  const requestedNames = parseRequestedTestNames(process.argv, testCases);
  const selectedCases = resolveSelectedTestCases(testCases, requestedNames);

  resetLocalReplica();

  runDfx(['deploy', 'backend'], {
    stdio: 'inherit',
  });

  console.log(`Running ${selectedCases.length} canister test case(s)`);

  for (const testCase of selectedCases) {
    await runCase(`${testCase.fullName} (${testCase.displayName})`, testCase.fn);
  }

  console.log('All canister integration checks passed');
}

await main();