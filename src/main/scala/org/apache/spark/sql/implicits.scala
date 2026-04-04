package org.apache.spark.sql

/** Scala 3 implicits for idiomatic Spark column references.
  *
  * Usage:
  * {{{
  *   import org.apache.spark.sql.implicits.*
  *
  *   val df = spark.range(10)
  *   df.select($"id", $"id" + 1)
  *   df.filter($"id" > 5)
  * }}}
  */
object implicits:

  /** String interpolator for column references: `$"colName"`.
    *
    * In Scala 3 we use an extension on StringContext to provide the `$` interpolator.
    */
  extension (sc: StringContext)
    def $(args: Any*): Column =
      // The typical usage is $"colName" which gives sc.parts = Seq("colName")
      // and args = Seq.empty. We join them for safety.
      val name = sc.s(args*)
      Column(name)

  /** Implicit conversion from Symbol to Column (Scala 2 compat style). */
  given Conversion[Symbol, Column] with
    def apply(s: Symbol): Column = Column(s.name)

  /** Implicit conversion from String to Column for select("colName") etc. */
  extension (colName: String)
    def col: Column = Column(colName)
