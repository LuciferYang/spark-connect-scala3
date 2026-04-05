# API Gaps: spark-connect-scala3 vs Official Spark Connect Client

This document tracks the remaining API gaps between spark-connect-scala3 (SC3) and the
official Apache Spark Connect client (`sql/api` + `sql/connect/client`).

Last updated: 2026-04-05

## Current Coverage

| Metric | Count |
|--------|-------|
| Official `functions.scala` unique function names | 542 |
| SC3 `functions.scala` unique function names | 542 |
| Coverage | **100%** |

## Remaining Gaps

### Functions

**All 542 official functions are covered.** No function-level gaps remain.

### Structural Gaps (Not Function Gaps)

These are deeper infrastructure differences that don't affect function coverage:

| Feature | Description | Priority |
|---------|-------------|----------|
| `Aggregator.toColumn` | Requires `TypedColumn` + `InvokeInlineUserDefinedFunction` column node layer | Low — typed Dataset path, niche use |

### UDAF Limitations

The `udaf` function is fully implemented and supports:
- `Aggregator[IN, BUF, OUT]` with primitive/boxed/string/binary/date/timestamp/decimal types
- Tuple buffer encoders via `Encoders.tuple(e1, e2, ...)`
- Case class encoders via `Encoders.product[T]`
- Registration via `spark.udf.register("name", udaf(agg, enc))`
- SQL usage after registration

Not supported:
- `Aggregator.toColumn` (typed Dataset path — requires `TypedColumn`)
- `ClosureCleaner` (SC3 UDFs also skip this, consistent behavior)

## Naming Differences (Not Functional Gaps)

These are internal helper methods with different names but equivalent functionality:

| Official | SC3 | Notes |
|----------|-----|-------|
| `createLambda` (private) | `createLambda1`, `createLambda2` (private) | Split into arity-specific versions |
| — | `callFn` (private) | SC3 helper for `UnresolvedFunction` construction |
| — | `callInternalFn` (private) | SC3 helper for internal functions (`is_internal=true`) |
| — | `lambdaVar` (private) | SC3 helper for lambda variable creation |

## Resolved Gaps

The following gaps were identified and resolved:

| Function | Resolution | Date |
|----------|-----------|------|
| `udaf` (2 overloads) | Implemented via `Aggregator` + `Encoders` factory + `UserDefinedFunction.forAggregator` | 2026-04-05 |
| `approxCountDistinct` (2 overloads) | Added as deprecated alias for `approx_count_distinct` | 2026-04-05 |
| `monotonicallyIncreasingId` | Added as deprecated alias for `monotonically_increasing_id` | 2026-04-05 |
| `typedlit` | Added as alias for `typedLit` | 2026-04-05 |
| `unwrap_udt` | Added with `callInternalFn` helper (`is_internal=true`) | 2026-04-05 |
| `udf` (Function0, Function6–Function10) | Extended from Function1–5 to Function0–10 | 2026-04-05 |
| ~250 functions (Batch 1+2) | Added across all categories: aggregate, datetime, string, math, collection, JSON, XML, URL, variant, conditional, misc, hash, bitwise, partition transform, datasketch, geospatial | 2026-04-05 |
