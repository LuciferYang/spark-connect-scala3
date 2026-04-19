package org.apache.spark.sql

import java.util.UUID
import java.util.concurrent.atomic.AtomicBoolean
import scala.concurrent.{Await, Future, Promise}
import scala.concurrent.duration.Duration

/** Helper class to simplify usage of `Dataset.observe(String, Column, Column*)`.
  *
  * {{{
  *   val observation = Observation("my metrics")
  *   val observed = ds.observe(observation, count(lit(1)).as("rows"), max($"id").as("maxid"))
  *   observed.write.parquet("ds.parquet")
  *   val metrics = observation.get
  * }}}
  *
  * Collects metrics during the first action on the observed dataset. Subsequent actions do not
  * modify the metrics. Retrieval via `get` blocks until the first action completes.
  *
  * @param name
  *   name of the observation
  */
final class Observation(val name: String):
  require(name.nonEmpty, "Name must not be empty")

  def this() = this(UUID.randomUUID().toString)

  private val registered = AtomicBoolean(false)
  private val promise = Promise[Row]()

  /** Future holding the (yet to be completed) observation. */
  val future: Future[Row] = promise.future

  /** Get the observed metrics as a `Map[String, Any]`.
    *
    * Blocks until the first action on the observed dataset completes.
    */
  @throws[InterruptedException]
  def get: Map[String, Any] =
    val row = Await.result(future, Duration.Inf)
    if row == null then Map.empty
    else
      row.schema match
        case Some(s) => row.getValuesMap[Any](s.fields.map(_.name).toSeq)
        case None    => Map.empty

  /** Get the observed metrics as a `java.util.Map[String, Any]`.
    *
    * Java-friendly variant of `get`. Blocks until the first action completes.
    */
  @throws[InterruptedException]
  def getAsJava: java.util.Map[String, Any] =
    import scala.jdk.CollectionConverters.*
    get.asJava

  /** Mark this Observation as registered. Each Observation can only be used once. */
  private[sql] def markRegistered(): Unit =
    if !registered.compareAndSet(false, true) then
      throw IllegalArgumentException(
        "An Observation can be used with a Dataset only once"
      )

  /** Complete the promise with the observed metrics row. */
  private[sql] def setMetrics(row: Row): Boolean =
    promise.trySuccess(row)

  /** The plan ID this observation is bound to (set after observe()). */
  @volatile private[sql] var planId: Long = -1L

object Observation:
  def apply(): Observation = Observation(UUID.randomUUID().toString)
  def apply(name: String): Observation = new Observation(name)
