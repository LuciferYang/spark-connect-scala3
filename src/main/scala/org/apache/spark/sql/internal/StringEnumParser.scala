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
    *   human-readable parameter name for error messages (e.g. "joinType", "save mode"). Used in
    *   both the null-error ("$paramName must not be null") and the unknown-error ("$errorVerb
    *   $paramName: ...") messages.
    * @param mapping
    *   accepted input keys (already normalized — see `trim`/`stripUnderscore`) to values
    * @param trim
    *   if true, strip leading/trailing whitespace before matching
    * @param stripUnderscore
    *   if true, remove all `_` from the input before matching
    * @param errorVerb
    *   first word of the unknown-value error message. Defaults to "Unknown". For API parity with
    *   upstream Apache Spark, `toJoinType` passes "Unsupported".
    * @param acceptedDisplay
    *   optional override for what to list after "Accepted values:" in the error message. Defaults
    *   to the sorted `mapping.keys`; callers with user-visible aliases can pass a grouped listing
    *   (e.g. include both `"left_outer"` and `"leftouter"` even though only the normalized form is
    *   in `mapping`).
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
      stripUnderscore: Boolean = false,
      errorVerb: String = "Unknown",
      acceptedDisplay: Option[Seq[String]] = None
  ): T =
    require(input != null, s"$paramName must not be null")
    val trimmed = if trim then input.trim else input
    val lowered = trimmed.toLowerCase(Locale.ROOT)
    val normalized = if stripUnderscore then lowered.replace("_", "") else lowered
    mapping.get(normalized) match
      case Some(v) => v
      case None    =>
        val accepted = acceptedDisplay match
          case Some(xs) if xs.nonEmpty => xs
          case _                       => mapping.keys.toSeq.sorted
        // Sanitize input for the error message: render control characters (newlines, tabs, …)
        // via their escape form so a malicious input cannot forge log lines or smuggle ANSI.
        val sanitized = input.flatMap {
          case c if c.isControl => f"\\u${c.toInt}%04x"
          case c                => c.toString
        }
        throw IllegalArgumentException(
          s"$errorVerb $paramName: '$sanitized'. Accepted values: " +
            accepted.mkString("'", "', '", "'") + "."
        )
