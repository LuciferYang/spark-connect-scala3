package org.apache.spark.sql.internal

import java.util.Locale

/** Parse a user-supplied string option into a typed enum value with consistent error messaging.
  *
  * Replaces the hand-rolled `toLowerCase(Locale.ROOT) match` + throw pattern duplicated across
  * `DataFrame.toJoinType`, `DataFrame.explain(mode)`, `DataFrameWriter.toProtoMode`, and
  * `DataFrameNaFunctions.drop(how)`.
  *
  * Normalization steps (applied in order):
  *   1. null check (throws IllegalArgumentException with helpful name)
  *   2. optional trim
  *   3. lower-case via Locale.ROOT (avoids Turkish locale bug)
  *   4. optional underscore strip (for joinType: "left_outer" → "leftouter")
  *
  * Lookup is a single `Map[String, T]` pass. Unknown inputs produce a uniform error message listing
  * accepted values.
  */
private[sql] object StringEnumParser:

  /** @param input
    *   raw user input (may be null)
    * @param paramName
    *   human-readable parameter name for error messages (e.g. "joinType", "save mode")
    * @param mapping
    *   accepted input keys (already normalized — see `trim`/`stripUnderscore`) to values
    * @param trim
    *   if true, strip leading/trailing whitespace before matching
    * @param stripUnderscore
    *   if true, remove all `_` from the input before matching
    * @tparam T
    *   the enum-like target type
    * @throws IllegalArgumentException
    *   on null input or unrecognized value
    */
  def parse[T](
      input: String,
      paramName: String,
      mapping: Map[String, T],
      trim: Boolean = false,
      stripUnderscore: Boolean = false
  ): T =
    require(input != null, s"$paramName must not be null")
    var normalized = input
    if trim then normalized = normalized.trim
    normalized = normalized.toLowerCase(Locale.ROOT)
    if stripUnderscore then normalized = normalized.replace("_", "")
    mapping.get(normalized) match
      case Some(v) => v
      case None    =>
        throw IllegalArgumentException(
          s"Unknown $paramName: '$input'. Accepted values: " +
            mapping.keys.toSeq.sorted.mkString("'", "', '", "'") + "."
        )
