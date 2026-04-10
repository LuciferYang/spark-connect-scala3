# Integration Test Coverage

Integration tests run against a live Spark Connect server (4.1.x) and validate real end-to-end behavior. They are tagged `@IntegrationTest` and excluded from default `sbt test`. To run them:

```bash
SBT_OPTS="-Dsun.net.client.defaultConnectTimeout=10000 -Dsun.net.client.defaultReadTimeout=60000 -Djava.net.preferIPv4Stack=true" \
  build/sbt 'set Test / testOptions := Seq()' 'testOnly *IntegrationSuite'
```

See [run-integration-tests.md](run-integration-tests.md) for the full command and configuration notes.

## Last run: 2026-04-10

```
Suites:    20 (0 aborted)
Tests:    454 total
  ✅  435 passed
  ❌    0 failed
  ⊘   19 canceled (server gaps + Scala 3 case-class typed lambdas)
```

Pass rate excluding cancels: **100% (435/435)**.

## Per-suite breakdown

| Suite | Tests | Canceled | Notes |
|-------|------:|---------:|-------|
| CatalogIntegrationSuite | 47 | 12 | server gaps (see § Cancel reasons) |
| ColumnIntegrationSuite | 38 | 1 | Array-of-aliases overload |
| DataFrameIntegrationSuite | 97 | 0 | |
| DatasetIntegrationSuite | 6 | 0 | |
| DatasetTypedOpsIntegrationSuite | 49 | 0 | |
| GroupedDataFrameIntegrationSuite | 13 | 0 | |
| JdbcIntegrationSuite | 13 | 0 | H2 read coverage |
| KeyValueGroupedDatasetIntegrationSuite | 10 | 3 | Scala 3 lambda (7 pass, 3 cancel) |
| NaStatIntegrationSuite | 20 | 0 | |
| ReadWriteIntegrationSuite | 25 | 0 | |
| RuntimeConfigIntegrationSuite | 11 | 0 | |
| SparkSessionIntegrationSuite | 26 | 1 | server hang on `interruptOperation` |
| StreamingIntegrationSuite | 2 | 0 | |
| StreamingReadWriteIntegrationSuite | 17 | 1 | `foreachBatch` lambda |
| SubqueryIntegrationSuite | 5 | 0 | |
| TableValuedFunctionIntegrationSuite | 21 | 1 | `variant_explode` server gap |
| UdfIntegrationSuite | 9 | 0 | |
| WindowIntegrationSuite | 10 | 0 | |
| WriterIntegrationSuite | 2 | 0 | |
| WriterV2IntegrationSuite | 33 | 0 | all pass (requires InMemoryRowLevelOperationTableCatalog) |
| **Total** | **454** | **19** | |

## Cancel reasons

### Scala 3 lambda serialization (4 tests)

Spark 4.0/4.1 servers are built with Scala 2.13 and cannot deserialize lambdas produced by Scala 3 (`$deserializeLambda$` is missing). All affected tests are wrapped in `withLambdaCompat` and gracefully `cancel`. They will start passing once a Scala 3-native Spark Connect server is available. See [README.md § Known Limitations](README.md#scala-3-lambda-serialization-on-scala-213-spark-server).

| Suite | Test |
|-------|------|
| KeyValueGroupedDatasetIntegrationSuite | reduceGroups reduces within each group |
| KeyValueGroupedDatasetIntegrationSuite | mapValues transforms values then mapGroups |
| KeyValueGroupedDatasetIntegrationSuite | groupByKey.agg with typed column |
| StreamingReadWriteIntegrationSuite | foreachBatch executes batch function |

### Server-side gaps requiring extended proto (13 tests)

These cancel because the running Spark 4.1.x server does not (yet) support the corresponding proto field, RPC, or feature. They are not SC3 client bugs.

| Suite | Test | Reason |
|-------|------|--------|
| CatalogIntegrationSuite | listViews | extended proto field |
| CatalogIntegrationSuite | listViews with dbName | extended proto field |
| CatalogIntegrationSuite | getFunction with name and dbName | built-in function resolution |
| CatalogIntegrationSuite | functionExists with name and dbName | built-in function resolution |
| CatalogIntegrationSuite | createDatabase and dropDatabase | not supported on default catalog |
| CatalogIntegrationSuite | dropTable | proto field 28 not supported on this server |
| CatalogIntegrationSuite | truncateTable | RPC missing |
| CatalogIntegrationSuite | analyzeTable | RPC missing |
| CatalogIntegrationSuite | getTableProperties | RPC missing |
| CatalogIntegrationSuite | getCreateTableString | RPC missing |
| CatalogIntegrationSuite | listPartitions | RPC missing |
| CatalogIntegrationSuite | dropView | not supported on default catalog (no Hive metastore) |
| TableValuedFunctionIntegrationSuite | tvf.variant_explode | variant type support |

### Server hang (1 test)

| Suite | Test | Reason |
|-------|------|--------|
| SparkSessionIntegrationSuite | interruptOperation returns without error for fake operation id | Spark 4.1.x server hangs on `INTERRUPT_TYPE_OPERATION_ID` with a non-existent operation id |

### Other (1 test)

| Suite | Test | Reason |
|-------|------|--------|
| ColumnIntegrationSuite | as with Array of aliases for struct column | requires struct column with multi-alias output |

## Genuinely uncovered API surface

Audit method: extracted every public `def` from 21 main API source files (587 public methods total) and cross-referenced against all 19 integration test files (6,129 lines). After the latest test additions, **14 public methods** are not exercised by any integration test, broken down by reason:

| Reason | Count | Methods |
|---|---:|---|
| **Blocked by Scala 3/2.13 lambda incompat** | 3 | `KeyValueGroupedDataset.{mapGroupsWithState, flatMapGroupsWithState, transformWithState}` |
| **Requires external resources** | 4 | `DataFrameWriter.jdbc` (external DB); `StreamingQueryManager.{addListener, removeListener, listListeners}` (gRPC listener stream + callback infra) |
| **Already exercised indirectly by test infra** | 4 | `SparkSession.{addArtifact, addClassDir}` (called by `IntegrationTestBase`); `Dataset.sparkSession` (trivial accessor); `SparkSession.setDefaultSession` (called by `Builder.build`) |
| **REPL-only / static lifecycle** | 2 | `SparkSession.registerClassFinder`, `SparkSession.clearDefaultSession` |
| **Deprecated upstream** | 1 | `Catalog.createExternalTable` |

**Method-level coverage: ~97.6% (573/587 public methods)**.

### Real test gaps

After this audit, the previously-uncovered Catalog methods `dropView`, `recoverPartitions`, and `refreshByPath` were added to `CatalogIntegrationSuite`. `recoverPartitions` and `refreshByPath` pass; `dropView` cancels because the test server has no Hive metastore.

There are no remaining "should add a test today" gaps. The remaining 14 uncovered methods are either blocked by external constraints (Scala 2.13 server, external DB, gRPC listener infrastructure), exercised indirectly by test infrastructure, or intentionally out of scope (deprecated, REPL tooling).
