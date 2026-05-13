package org.apache.spark.sql.internal

/** JSON escape utility used by `Row.json` / `Row.prettyJson` and
  * `StreamingQueryListener.{QueryStartedEvent,QueryTerminatedEvent}.json`.
  *
  * Handles the set of characters the JSON spec requires to be escaped (quote, backslash, control
  * chars), plus U+2028 / U+2029 line separators which — though valid in JSON — break when the
  * output is consumed as JavaScript (where both are newline terminators).
  *
  * Does NOT double-encode already-escaped input. Does NOT wrap the output in quotes; callers do
  * that themselves.
  */
private[sql] object JsonEscaping:

  /** Escape a string for safe inclusion between JSON string quotes. */
  def escape(s: String): String =
    if s == null then return "null"
    val sb = new StringBuilder(s.length)
    var i = 0
    while i < s.length do
      val ch = s.charAt(i)
      ch match
        case '"'          => sb.append("\\\"")
        case '\\'         => sb.append("\\\\")
        case '\b'         => sb.append("\\b")
        case '\f'         => sb.append("\\f")
        case '\n'         => sb.append("\\n")
        case '\r'         => sb.append("\\r")
        case '\t'         => sb.append("\\t")
        case ' '     => sb.append("\\u2028")
        case ' '     => sb.append("\\u2029")
        case c if c < ' ' =>
          sb.append("\\u")
          sb.append(f"${c.toInt}%04x")
        case c            => sb.append(c)
      i += 1
    sb.toString
