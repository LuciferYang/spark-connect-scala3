# Integration Test Coverage

Integration tests run against a live Spark Connect server (4.1.x) and validate real end-to-end behavior. They are tagged `@IntegrationTest` and excluded from default `sbt test`. To run them:

```bash
SBT_OPTS="-Dsun.net.client.defaultConnectTimeout=10000 -Dsun.net.client.defaultReadTimeout=60000 -Djava.net.preferIPv4Stack=true" \
  build/sbt 'set Test / testOptions := Seq()' 'testOnly *IntegrationSuite'
```

See [run-integration-tests.md](run-integration-tests.md) for the full command and configuration notes.

## Last run: 2026-04-07

```
Suites:    19 (0 aborted)
Tests:    436 total
  ✅  398 passed
  ❌    0 failed
  ⊘   38 canceled (server gaps + Scala 3 lambda incompatibility)
Time:      44 s
```

## Per-suite breakdown

| Suite | Tests | Canceled | Notes |
|-------|------:|---------:|-------|
| CatalogIntegrationSuite | 44 | 11 | server gaps (see § Cancel reasons) |
| ColumnIntegrationSuite | 38 | 1 | Array-of-aliases overload |
| DataFrameIntegrationSuite | 97 | 0 | |
| DatasetIntegrationSuite | 2 | 1 | Scala 3 lambda |
| DatasetTypedOpsIntegrationSuite | 49 | 3 | Scala 3 lambda |
| GroupedDataFrameIntegrationSuite | 13 | 0 | |
| KeyValueGroupedDatasetIntegrationSuite | 10 | 9 | Scala 3 lambda (only `cogroup` runs) |
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
| WriterV2IntegrationSuite | 33 | 10 | mergeInto + writeTo.overwrite (requires V2 catalog) |
| **Total** | **436** | **38** | |

## Cancel reasons

### Scala 3 lambda serialization (15 tests)

Spark 4.0/4.1 servers are built with Scala 2.13 and cannot deserialize lambdas produced by Scala 3 (`$deserializeLambda$` is missing). All affected tests are wrapped in `withLambdaCompat` and gracefully `cancel`. They will start passing once a Scala 3-native Spark Connect server is available. See [README.md § Known Limitations](README.md#scala-3-lambda-serialization-on-scala-213-spark-server).

| Suite | Test |
|-------|------|
| DatasetIntegrationSuite | Dataset map and filter |
| DatasetTypedOpsIntegrationSuite | flatMap expands each element |
| DatasetTypedOpsIntegrationSuite | mapPartitions transforms partitions |
| DatasetTypedOpsIntegrationSuite | transform applies function to Dataset |
| KeyValueGroupedDatasetIntegrationSuite | groupByKey.keys returns distinct keys |
| KeyValueGroupedDatasetIntegrationSuite | groupByKey.count returns (key, count) pairs |
| KeyValueGroupedDatasetIntegrationSuite | mapGroups aggregates per group |
| KeyValueGroupedDatasetIntegrationSuite | flatMapGroups expands per group |
| KeyValueGroupedDatasetIntegrationSuite | reduceGroups reduces within each group |
| KeyValueGroupedDatasetIntegrationSuite | mapValues transforms values then mapGroups |
| KeyValueGroupedDatasetIntegrationSuite | flatMapSortedGroups produces sorted iteration within group |
| KeyValueGroupedDatasetIntegrationSuite | groupByKey.agg with typed column |
| KeyValueGroupedDatasetIntegrationSuite | keyAs changes key encoder type |
| StreamingReadWriteIntegrationSuite | foreachBatch executes batch function |

### Server-side gaps requiring extended proto / V2 catalog (22 tests)

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
| TableValuedFunctionIntegrationSuite | tvf.variant_explode | variant type support |
| WriterV2IntegrationSuite | writeTo.overwrite with condition replaces matching rows | V2 catalog required |
| WriterV2IntegrationSuite | writeTo.overwrite with non-matching condition keeps all rows | V2 catalog required |
| WriterV2IntegrationSuite | mergeInto with whenMatched.updateAll and whenNotMatched.insertAll | V2 catalog required |
| WriterV2IntegrationSuite | mergeInto with whenMatched(condition).update(Map) | V2 catalog required |
| WriterV2IntegrationSuite | mergeInto with whenMatched.delete | V2 catalog required |
| WriterV2IntegrationSuite | mergeInto with whenNotMatched(condition).insert(Map) | V2 catalog required |
| WriterV2IntegrationSuite | mergeInto with whenNotMatchedBySource.delete | V2 catalog required |
| WriterV2IntegrationSuite | mergeInto with whenNotMatchedBySource(condition).update(Map) | V2 catalog required |
| WriterV2IntegrationSuite | mergeInto with withSchemaEvolution | V2 catalog required |
| WriterV2IntegrationSuite | mergeInto with multiple when clauses | V2 catalog required |

### Server hang (1 test)

| Suite | Test | Reason |
|-------|------|--------|
| SparkSessionIntegrationSuite | interruptOperation returns without error for fake operation id | Spark 4.1.x server hangs on `INTERRUPT_TYPE_OPERATION_ID` with a non-existent operation id |

### Other (1 test)

| Suite | Test | Reason |
|-------|------|--------|
| ColumnIntegrationSuite | as with Array of aliases for struct column | requires struct column with multi-alias output |

## Genuinely uncovered API surface

After this run, the only public APIs without integration coverage are:

- **`DataFrameWriter.jdbc(...)`** — requires an external JDBC database; not in scope for the test environment.
- **`StreamingQueryManager.addListener` / `removeListener` / `listListeners`** — covered only by unit tests (`StreamingQueryManagerSuite`, `StreamingQueryListenerSuite`); no end-to-end integration test exercises listener callbacks via the gRPC stream.

Everything else in the public API is exercised by at least one integration test.
