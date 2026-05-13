package org.apache.spark.sql.internal

/** Shared builder-style `option(...)` DSL for `DataFrameReader`, `DataFrameWriter`, and
  * `DataFrameWriterV2`.
  *
  * Each class implements the String-valued `option(key, value: String): Self` operation (which
  * stores the option in its own collection); the primitive-typed overloads (`Boolean`, `Long`,
  * `Double`) are implemented once here via `.toString` to eliminate the 9-line duplication
  * across the three classes.
  *
  * @tparam Self the concrete builder type (self-typed via F-bound) so chained calls return the
  *   correct subtype for further method access.
  */
private[sql] trait OptionBuilder[Self]:
  def option(key: String, value: String): Self

  def option(key: String, value: Boolean): Self = option(key, value.toString)
  def option(key: String, value: Long): Self = option(key, value.toString)
  def option(key: String, value: Double): Self = option(key, value.toString)
