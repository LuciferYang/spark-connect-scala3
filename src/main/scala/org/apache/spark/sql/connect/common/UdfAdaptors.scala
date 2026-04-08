package org.apache.spark.sql.connect.common

/** Top-level (non-anonymous, non-inner) adaptor classes that wrap user-provided functions for
  * Dataset typed operations like `map`, `filter`, and `flatMap`.
  *
  * Why these exist: SC3 is compiled with Scala 3, but Spark 4.0/4.1 servers run Scala 2.13.
  * Anonymous lambdas defined inside `Dataset.map` / `Dataset.filter` capture the enclosing
  * method's parameters as closure state, producing a synthesized class whose layout the
  * Scala 2.13 server's `ObjectInputStream` cannot reconstruct ("Failed to unpack scala udf").
  *
  * Top-level classes with named, stable bytecode emit a deterministic class layout that the
  * server can decode reliably. The user's function is held as a `val` field instead of being
  * captured by an inner closure.
  *
  * NOTE: These adaptors solve the wrapping-closure problem (Problem A in INTEGRATION-TEST-COVERAGE).
  * They do NOT solve user-type field access (Problem B), where the user's lambda itself
  * references methods of a user-defined case class — that path still fails on the Scala 2.13
  * server.
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

/** Adaptor for `Dataset.flatMap(func: T => IterableOnce[U])` — flattens results into a single iterator. */
final class FlatMapAdaptor[T, U](val func: T => IterableOnce[U])
    extends (Iterator[T] => Iterator[U])
    with Serializable:
  def apply(iter: Iterator[T]): Iterator[U] = iter.flatMap(func)
