# API Gaps: spark-connect-scala3 vs Official Spark Connect Client

This document tracks the remaining API gaps between spark-connect-scala3 (SC3) and the
official Apache Spark Connect client (`sql/api` + `sql/connect/client`).

Last updated: 2026-04-05

## Current Coverage

| Metric | Count |
|--------|-------|
| Official `functions.scala` unique function names | 542 |
| SC3 `functions.scala` unique function names | 541 |
| Coverage | **99.8%** |

## Remaining Gaps

### 1. `udaf` — User-Defined Aggregate Function

**Status**: Not implemented
**Difficulty**: High — requires 5 missing infrastructure components
**Priority**: Medium (niche use case, but part of public API)

#### What `udaf` Does

`udaf` converts a user-defined `Aggregator[IN, BUF, OUT]` object into a `Column` expression
that can be used in `groupBy(...).agg(...)`. The official implementation:

1. Takes an `Aggregator[IN, BUF, OUT]` instance
2. Serializes the `Aggregator` + its `Encoder`s to bytes via Java serialization
3. Wraps the bytes in a `ScalarScalaUDF` proto message
4. Sends the proto as a `TypedAggregateExpression` to the Spark Connect server
5. The server deserializes and executes the aggregation

#### Required Infrastructure (5 components)

Each component builds on the previous:

```
udaf() function
  └── Aggregator[IN, BUF, OUT] trait
        └── Encoder[T] (full serialization, not just type inference)
              └── ExpressionEncoder[T]
                    └── Catalyst expression tree serialization
                          └── JVM object serialization for ScalarScalaUDF.payload
```

| # | Component | Description | Official Location | SC3 Status |
|---|-----------|-------------|-------------------|------------|
| 1 | **`Aggregator[IN, BUF, OUT]`** | Abstract class defining `zero`, `reduce`, `merge`, `finish`, `bufferEncoder`, `outputEncoder` | `sql/api: o.a.s.sql.expressions.Aggregator` | Missing |
| 2 | **`Encoder[T]` (serialization)** | Full encoder that can serialize/deserialize JVM objects to Spark's internal row format. SC3 currently has `Encoder` but only for `sparkTypeOf` (type inference), not actual serde. | `sql/api: o.a.s.sql.Encoder` | Partial — type inference only |
| 3 | **`ExpressionEncoder[T]`** | Concrete `Encoder` implementation using Catalyst expression trees for encoding/decoding. This is the core serialization mechanism. | `sql/catalyst: o.a.s.sql.catalyst.encoders.ExpressionEncoder` | Missing |
| 4 | **Catalyst expression tree serde** | Serialization of Catalyst `Expression` nodes used by `ExpressionEncoder`. Needed to build the `ScalarScalaUDF` payload. | `sql/catalyst: o.a.s.sql.catalyst.expressions.*` | Missing |
| 5 | **`UdfPacket` / JVM serialization** | Java serialization of the `Aggregator` + encoder objects into `bytes` for the `ScalarScalaUDF.payload` proto field. SC3 has `UdfPacket` for simple UDFs but not for aggregators. | `sql/connect/common: UdfPacket` | Partial — UDF only, no aggregator |

#### Proto Support

The proto definition already exists in SC3:

```protobuf
// expressions.proto
message TypedAggregateExpression {
  ScalarScalaUDF scalar_scala_udf = 1;
}

message ScalarScalaUDF {
  bytes payload = 1;           // Serialized Aggregator + Encoders
  repeated DataType inputTypes = 2;
  DataType outputType = 3;
  bool nullable = 4;
  bool aggregate = 5;          // Set to true for udaf
}
```

The proto infrastructure is ready. The missing piece is the client-side Scala code to
populate `ScalarScalaUDF.payload` with a serialized `Aggregator`.

#### Implementation Path

A phased approach to implementing `udaf`:

**Phase 1: `Aggregator` trait** (standalone, no dependencies)
- Port `o.a.s.sql.expressions.Aggregator` abstract class
- Define `zero`, `reduce`, `merge`, `finish`, `bufferEncoder`, `outputEncoder`
- Adapt for Scala 3 (use `derives Encoder` instead of implicit `Encoder`)

**Phase 2: Extend `Encoder` with serialization**
- Extend the existing SC3 `Encoder[T]` to support actual serialization
- This is the hardest part — `ExpressionEncoder` depends on Catalyst internals
- Alternative: implement a simplified serialization using Arrow or Java serialization
  directly, bypassing `ExpressionEncoder`

**Phase 3: `udaf` function + serialization**
- Serialize `Aggregator` + encoders into `ScalarScalaUDF.payload`
- Build `TypedAggregateExpression` proto
- Wire into `functions.udaf()`

**Alternative approach**: Instead of porting the full `ExpressionEncoder` stack, investigate
whether the Spark Connect server can accept a simpler serialization format. The server
already deserializes `UdfPacket` for simple UDFs — extending that mechanism to aggregators
may be feasible with less infrastructure.

## Naming Differences (Not Functional Gaps)

These are internal helper methods with different names but equivalent functionality:

| Official | SC3 | Notes |
|----------|-----|-------|
| `createLambda` (private) | `createLambda1`, `createLambda2` (private) | Split into arity-specific versions |
| — | `callFn` (private) | SC3 helper for `UnresolvedFunction` construction |
| — | `callInternalFn` (private) | SC3 helper for internal functions (`is_internal=true`) |
| — | `lambdaVar` (private) | SC3 helper for lambda variable creation |

## Resolved Gaps (Completed 2026-04-05)

The following gaps were identified and resolved:

| Function | Resolution |
|----------|-----------|
| `approxCountDistinct` (2 overloads) | Added as deprecated alias for `approx_count_distinct` |
| `monotonicallyIncreasingId` | Added as deprecated alias for `monotonically_increasing_id` |
| `typedlit` | Added as alias for `typedLit` |
| `unwrap_udt` | Added with `callInternalFn` helper (`is_internal=true`) |
| `udf` (Function0, Function6–Function10) | Extended from Function1–5 to Function0–10 |
| ~250 functions (Batch 1+2) | Added across all categories: aggregate, datetime, string, math, collection, JSON, XML, URL, variant, conditional, misc, hash, bitwise, partition transform, datasketch, geospatial |
