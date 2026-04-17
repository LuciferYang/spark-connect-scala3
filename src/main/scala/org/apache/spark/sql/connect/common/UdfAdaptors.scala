package org.apache.spark.sql.connect.common

/** Top-level (non-anonymous, non-inner) adaptor classes that wrap user-provided functions for
  * Dataset typed operations like `map`, `filter`, and `flatMap`.
  *
  * Why these exist: SC3 is compiled with Scala 3, but Spark 4.0/4.1 servers run Scala 2.13.
  * Anonymous lambdas defined inside `Dataset.map` / `Dataset.filter` capture the enclosing method's
  * parameters as closure state, producing a synthesized class whose layout the Scala 2.13 server's
  * `ObjectInputStream` cannot reconstruct ("Failed to unpack scala udf").
  *
  * Top-level classes with named, stable bytecode emit a deterministic class layout that the server
  * can decode reliably. The user's function is held as a `val` field instead of being captured by
  * an inner closure.
  *
  * NOTE: These adaptors solve the wrapping-closure problem (Problem A in
  * INTEGRATION-TEST-COVERAGE). They do NOT solve user-type field access (Problem B), where the
  * user's lambda itself references methods of a user-defined case class — that path still fails on
  * the Scala 2.13 server.
  */

/** Adaptor for `Dataset.map(func: T => U)` — applies `func` to each element of an iterator. */
final class MapPartitionsAdaptor[T, U](val func: T => U)
    extends (Iterator[T] => Iterator[U])
    with Serializable:
  def apply(iter: Iterator[T]): Iterator[U] = iter.map(func)

/** Adaptor for `Dataset.filter(func: T => Boolean)` — keeps elements where `func` returns true. */
final class FilterAdaptor[T](val func: T => Boolean)
    extends (Iterator[T] => Iterator[T])
    with Serializable:
  def apply(iter: Iterator[T]): Iterator[T] = iter.filter(func)

/** Adaptor for `Dataset.flatMap(func: T => IterableOnce[U])` — flattens results into a single
  * iterator.
  */
final class FlatMapAdaptor[T, U](val func: T => IterableOnce[U])
    extends (Iterator[T] => Iterator[U])
    with Serializable:
  def apply(iter: Iterator[T]): Iterator[U] = iter.flatMap(func)

// ---------------------------------------------------------------------------
// KeyValueGroupedDataset adaptors
// ---------------------------------------------------------------------------

/** Adaptor for `KVGD.mapGroups` — wraps a `(K, Iterator[V]) => U` function into a
  * `(K, Iterator[V]) => IterableOnce[U]` by producing `Iterator.single(func(k, iter))`.
  *
  * Without this, the wrapper lambda `(k, iter) => Iterator.single(func(k, iter))` compiles into the
  * KVGD class, whose impl method does not exist on the Scala 2.13 server.
  */
final class MapGroupsAdaptor[K, V, U](val func: (K, Iterator[V]) => U)
    extends ((K, Iterator[V]) => IterableOnce[U])
    with Serializable:
  def apply(k: K, iter: Iterator[V]): IterableOnce[U] = Iterator.single(func(k, iter))

/** Adaptor for `KVGD.count()` — maps each group to `(key, count)`. */
final class CountGroupsAdaptor[K]
    extends ((K, Iterator[?]) => (K, Long))
    with Serializable:
  def apply(k: K, iter: Iterator[?]): (K, Long) = (k, iter.size.toLong)

/** Adaptor for `KVGD.reduceGroups` fallback — reduces values within each group and pairs with key.
  */
final class ReduceGroupsAdaptor[K, V](val func: (V, V) => V)
    extends ((K, Iterator[V]) => (K, V))
    with Serializable:
  def apply(k: K, iter: Iterator[V]): (K, V) = (k, iter.reduce(func))

/** Adaptor for `KVGD.flatMapGroups` after `mapValues` — composes the value transform with the
  * user's flatMap function.
  *
  * After `mapValues(f)`, the GroupMap input relation still has the original value type V_orig. The
  * server deserializes each row as V_orig, but the user's function expects the mapped type W. This
  * adaptor bridges the gap by applying `valueMapFunc` to each element before delegating to the
  * user's `flatMapFunc`.
  */
final class MapValuesFlatMapAdaptor[K, U](
    val valueMapFunc: Any => Any,
    val flatMapFunc: (K, Iterator[Any]) => IterableOnce[U]
) extends ((K, Iterator[Any]) => IterableOnce[U])
    with Serializable:
  def apply(k: K, iter: Iterator[Any]): IterableOnce[U] =
    flatMapFunc(k, iter.map(valueMapFunc))

/** Adaptor for `KVGD.cogroup` — wraps a `(K, Iterator[V], Iterator[U]) => IterableOnce[R]` function
  * into a top-level serializable Function3.
  *
  * Without this, the raw Scala 3 lambda serializes as `LambdaSerializationProxy` which the Scala
  * 2.13 server cannot cast to `scala.Function3`.
  */
final class CoGroupAdaptor[K, V, U, R](
    val func: (K, Iterator[V], Iterator[U]) => IterableOnce[R]
) extends ((K, Iterator[V], Iterator[U]) => IterableOnce[R])
    with Serializable:
  def apply(k: K, left: Iterator[V], right: Iterator[U]): IterableOnce[R] =
    func(k, left, right)
