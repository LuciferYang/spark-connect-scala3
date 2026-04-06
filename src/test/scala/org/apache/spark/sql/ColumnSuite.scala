package org.apache.spark.sql

import org.apache.spark.connect.proto.Expression
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import scala.jdk.CollectionConverters.*

class ColumnSuite extends AnyFunSuite with Matchers:

  test("Column from name creates UnresolvedAttribute") {
    val c = Column("id")
    c.expr.hasUnresolvedAttribute shouldBe true
    c.expr.getUnresolvedAttribute.getUnparsedIdentifier shouldBe "id"
  }

  test("Column.lit creates Literal for primitives") {
    val intCol = Column.lit(42)
    intCol.expr.hasLiteral shouldBe true
    val lit = intCol.expr.getLiteral
    lit.hasInteger shouldBe true

    val strCol = Column.lit("hello")
    val slit = strCol.expr.getLiteral
    slit.hasString shouldBe true

    val boolCol = Column.lit(true)
    val blit = boolCol.expr.getLiteral
    blit.hasBoolean shouldBe true
  }

  test("Column.lit(null) creates Null literal") {
    val c = Column.lit(null)
    val lit = c.expr.getLiteral
    lit.hasNull shouldBe true
  }

  test("Column.lit passes through Column") {
    val original = Column("x")
    Column.lit(original) should be theSameInstanceAs original
  }

  test("comparison operators create UnresolvedFunction") {
    val c = Column("a") === Column("b")
    c.expr.hasUnresolvedFunction shouldBe true
    val fn = c.expr.getUnresolvedFunction
    fn.getFunctionName shouldBe "=="
    fn.getArgumentsList should have size 2
  }

  test("arithmetic operators") {
    val c = Column("x") + Column("y")
    val fn = c.expr.getUnresolvedFunction
    fn.getFunctionName shouldBe "+"
  }

  test("logical operators") {
    val c = Column("a") && Column("b")
    val fn = c.expr.getUnresolvedFunction
    fn.getFunctionName shouldBe "and"
  }

  test("unary not") {
    val c = !Column("flag")
    val fn = c.expr.getUnresolvedFunction
    fn.getFunctionName shouldBe "not"
    fn.getArgumentsList should have size 1
  }

  test("cast creates Cast expression") {
    val c = Column("x").cast("string")
    c.expr.hasCast shouldBe true
  }

  test("alias creates Alias expression") {
    val c = Column("x").as("renamed")
    c.expr.hasAlias shouldBe true
    val alias = c.expr.getAlias
    alias.getNameList.asScala.toSeq shouldBe Seq("renamed")
  }

  test("asc / desc create SortOrder") {
    val asc = Column("x").asc
    asc.expr.hasSortOrder shouldBe true
    val so = asc.expr.getSortOrder
    so.getDirection shouldBe Expression.SortOrder.SortDirection.SORT_DIRECTION_ASCENDING

    val desc = Column("x").desc
    val dso = desc.expr.getSortOrder
    dso.getDirection shouldBe Expression.SortOrder.SortDirection.SORT_DIRECTION_DESCENDING
  }

  test("isNull / isNotNull / isNaN") {
    val c1 = Column("x").isNull
    c1.expr.getUnresolvedFunction.getFunctionName shouldBe "isnull"

    val c2 = Column("x").isNotNull
    c2.expr.getUnresolvedFunction.getFunctionName shouldBe "isnotnull"

    val c3 = Column("x").isNaN
    c3.expr.getUnresolvedFunction.getFunctionName shouldBe "isnan"
  }

  test("isin creates 'in' function") {
    val c = Column("x").isin(1, 2, 3)
    val fn = c.expr.getUnresolvedFunction
    fn.getFunctionName shouldBe "in"
    fn.getArgumentsList should have size 4 // column + 3 literals
  }

  test("like / rlike") {
    val c = Column("name").like("%test%")
    c.expr.getUnresolvedFunction.getFunctionName shouldBe "like"

    val c2 = Column("name").rlike("^test.*")
    c2.expr.getUnresolvedFunction.getFunctionName shouldBe "rlike"
  }

  test("contains / startsWith / endsWith") {
    Column("x").contains("a").expr
      .getUnresolvedFunction.getFunctionName shouldBe "contains"
    Column("x").startsWith("a").expr
      .getUnresolvedFunction.getFunctionName shouldBe "startswith"
    Column("x").endsWith("a").expr
      .getUnresolvedFunction.getFunctionName shouldBe "endswith"
  }

  // ----- Phase 2 tests -----

  test("when / otherwise chaining") {
    val c = functions.when(Column("x") > 0, "positive")
      .when(Column("x") === 0, "zero")
      .otherwise("negative")
    val fn = c.expr.getUnresolvedFunction
    fn.getFunctionName shouldBe "when"
    // when(cond1, val1, cond2, val2, defaultVal) = 5 args
    fn.getArgumentsList should have size 5
  }

  test("when without prior when throws") {
    assertThrows[IllegalArgumentException] {
      Column("x").when(Column("y"), 1)
    }
  }

  test("otherwise without prior when throws") {
    assertThrows[IllegalArgumentException] {
      Column("x").otherwise(1)
    }
  }

  test("getItem creates UnresolvedExtractValue") {
    val c = Column("m").getItem("key")
    c.expr.hasUnresolvedExtractValue shouldBe true
    val ev = c.expr.getUnresolvedExtractValue
    ev.getChild.hasUnresolvedAttribute shouldBe true
    ev.getExtraction.hasLiteral shouldBe true
  }

  test("getField creates UnresolvedExtractValue") {
    val c = Column("struct_col").getField("name")
    c.expr.hasUnresolvedExtractValue shouldBe true
  }

  test("apply is same as getItem") {
    val c = Column("arr")(0)
    c.expr.hasUnresolvedExtractValue shouldBe true
  }

  test("withField creates UpdateFields") {
    val c = Column("s").withField("new_field", Column.lit(42))
    c.expr.hasUpdateFields shouldBe true
    val uf = c.expr.getUpdateFields
    uf.getFieldName shouldBe "new_field"
    uf.hasValueExpression shouldBe true
  }

  test("dropFields creates UpdateFields chain") {
    val c = Column("s").dropFields("a", "b")
    c.expr.hasUpdateFields shouldBe true
    val uf = c.expr.getUpdateFields
    uf.getFieldName shouldBe "b"
    // Inner should also be UpdateFields
    uf.getStructExpression.hasUpdateFields shouldBe true
  }

  // ---------- Phase 4.1 tests ----------

  test("<=> builds null-safe equality function") {
    val c = Column("a") <=> "hello"
    c.expr.hasUnresolvedFunction shouldBe true
    c.expr.getUnresolvedFunction.getFunctionName shouldBe "<=>"
    c.expr.getUnresolvedFunction.getArgumentsCount shouldBe 2
  }

  test("eqNullSafe delegates to <=>") {
    val c = Column("a").eqNullSafe(42)
    c.expr.hasUnresolvedFunction shouldBe true
    c.expr.getUnresolvedFunction.getFunctionName shouldBe "<=>"
  }

  test("cast(DataType) uses Expression.Cast with type proto") {
    val c = Column("a").cast(types.IntegerType)
    c.expr.hasCast shouldBe true
    c.expr.getCast.hasType shouldBe true
  }

  test("cast(String) uses Expression.Cast with type_str") {
    val c = Column("a").cast("int")
    c.expr.hasCast shouldBe true
    c.expr.getCast.hasTypeStr shouldBe true
    c.expr.getCast.getTypeStr shouldBe "int"
  }

  test("try_cast(String) uses EVAL_MODE_TRY") {
    val c = Column("a").try_cast("int")
    c.expr.hasCast shouldBe true
    c.expr.getCast.getEvalMode shouldBe Expression.Cast.EvalMode.EVAL_MODE_TRY
    c.expr.getCast.hasTypeStr shouldBe true
  }

  test("try_cast(DataType) uses EVAL_MODE_TRY with type proto") {
    val c = Column("a").try_cast(types.LongType)
    c.expr.hasCast shouldBe true
    c.expr.getCast.getEvalMode shouldBe Expression.Cast.EvalMode.EVAL_MODE_TRY
    c.expr.getCast.hasType shouldBe true
  }

  test("ilike builds case-insensitive like function") {
    val c = Column("a").ilike("%hello%")
    c.expr.hasUnresolvedFunction shouldBe true
    c.expr.getUnresolvedFunction.getFunctionName shouldBe "ilike"
  }

  test("as[U: Encoder] returns a TypedColumn") {
    val tc = Column("a").as[Int]
    tc shouldBe a[TypedColumn[?, ?]]
    tc.encoder should not be null
  }

  // ---------- Coverage boost: Column.lit for all primitive types ----------

  test("Column.lit creates Literal for Byte") {
    val c = Column.lit(42.toByte)
    c.expr.hasLiteral shouldBe true
    c.expr.getLiteral.hasByte shouldBe true
    c.expr.getLiteral.getByte shouldBe 42
  }

  test("Column.lit creates Literal for Short") {
    val c = Column.lit(123.toShort)
    c.expr.hasLiteral shouldBe true
    c.expr.getLiteral.hasShort shouldBe true
    c.expr.getLiteral.getShort shouldBe 123
  }

  test("Column.lit creates Literal for Long") {
    val c = Column.lit(999L)
    c.expr.hasLiteral shouldBe true
    c.expr.getLiteral.hasLong shouldBe true
    c.expr.getLiteral.getLong shouldBe 999L
  }

  test("Column.lit creates Literal for Float") {
    val c = Column.lit(1.5f)
    c.expr.hasLiteral shouldBe true
    c.expr.getLiteral.hasFloat shouldBe true
    c.expr.getLiteral.getFloat shouldBe 1.5f
  }

  test("Column.lit creates Literal for Double") {
    val c = Column.lit(3.14)
    c.expr.hasLiteral shouldBe true
    c.expr.getLiteral.hasDouble shouldBe true
    c.expr.getLiteral.getDouble shouldBe 3.14
  }

  test("Column.lit falls back to String for unknown types") {
    val c = Column.lit(java.util.UUID.fromString("00000000-0000-0000-0000-000000000000"))
    c.expr.hasLiteral shouldBe true
    c.expr.getLiteral.hasString shouldBe true
    c.expr.getLiteral.getString shouldBe "00000000-0000-0000-0000-000000000000"
  }

  // ---------- Coverage boost: comparison operators with Any ----------

  test("=== with Any literal") {
    val c = Column("a") === 42
    c.expr.hasUnresolvedFunction shouldBe true
    c.expr.getUnresolvedFunction.getFunctionName shouldBe "=="
    c.expr.getUnresolvedFunction.getArgumentsCount shouldBe 2
  }

  test("=!= with Any literal") {
    val c = Column("a") =!= "hello"
    c.expr.hasUnresolvedFunction shouldBe true
    c.expr.getUnresolvedFunction.getFunctionName shouldBe "!="
  }

  test("> with Any literal") {
    val c = Column("a") > 10
    c.expr.getUnresolvedFunction.getFunctionName shouldBe ">"
  }

  test(">= with Any literal") {
    val c = Column("a") >= 10
    c.expr.getUnresolvedFunction.getFunctionName shouldBe ">="
  }

  test("< with Any literal") {
    val c = Column("a") < 10
    c.expr.getUnresolvedFunction.getFunctionName shouldBe "<"
  }

  test("<= with Any literal") {
    val c = Column("a") <= 10
    c.expr.getUnresolvedFunction.getFunctionName shouldBe "<="
  }

  // ---------- Coverage boost: Java-friendly comparison aliases ----------

  test("equalTo delegates to ===") {
    val c = Column("a").equalTo(42)
    c.expr.getUnresolvedFunction.getFunctionName shouldBe "=="
  }

  test("notEqual delegates to =!=") {
    val c = Column("a").notEqual(42)
    c.expr.getUnresolvedFunction.getFunctionName shouldBe "!="
  }

  test("gt delegates to >") {
    val c = Column("a").gt(10)
    c.expr.getUnresolvedFunction.getFunctionName shouldBe ">"
  }

  test("lt delegates to <") {
    val c = Column("a").lt(10)
    c.expr.getUnresolvedFunction.getFunctionName shouldBe "<"
  }

  test("geq delegates to >=") {
    val c = Column("a").geq(10)
    c.expr.getUnresolvedFunction.getFunctionName shouldBe ">="
  }

  test("leq delegates to <=") {
    val c = Column("a").leq(10)
    c.expr.getUnresolvedFunction.getFunctionName shouldBe "<="
  }

  // ---------- Coverage boost: logical operators ----------

  test("|| creates 'or' function") {
    val c = Column("a") || Column("b")
    c.expr.getUnresolvedFunction.getFunctionName shouldBe "or"
    c.expr.getUnresolvedFunction.getArgumentsCount shouldBe 2
  }

  test("and is alias for &&") {
    val c = Column("a").and(Column("b"))
    c.expr.getUnresolvedFunction.getFunctionName shouldBe "and"
  }

  test("or is alias for ||") {
    val c = Column("a").or(Column("b"))
    c.expr.getUnresolvedFunction.getFunctionName shouldBe "or"
  }

  // ---------- Coverage boost: arithmetic operators ----------

  test("- operator creates minus function") {
    val c = Column("x") - Column("y")
    c.expr.getUnresolvedFunction.getFunctionName shouldBe "-"
  }

  test("* operator creates multiply function") {
    val c = Column("x") * Column("y")
    c.expr.getUnresolvedFunction.getFunctionName shouldBe "*"
  }

  test("/ operator creates divide function") {
    val c = Column("x") / Column("y")
    c.expr.getUnresolvedFunction.getFunctionName shouldBe "/"
  }

  test("% operator creates modulo function") {
    val c = Column("x") % Column("y")
    c.expr.getUnresolvedFunction.getFunctionName shouldBe "%"
  }

  test("unary_- creates negative function") {
    val c = -Column("x")
    c.expr.getUnresolvedFunction.getFunctionName shouldBe "negative"
    c.expr.getUnresolvedFunction.getArgumentsCount shouldBe 1
  }

  // ---------- Coverage boost: Java-friendly arithmetic ----------

  test("plus adds with Any literal") {
    val c = Column("x").plus(5)
    c.expr.getUnresolvedFunction.getFunctionName shouldBe "+"
  }

  test("minus subtracts with Any literal") {
    val c = Column("x").minus(5)
    c.expr.getUnresolvedFunction.getFunctionName shouldBe "-"
  }

  test("multiply with Any literal") {
    val c = Column("x").multiply(5)
    c.expr.getUnresolvedFunction.getFunctionName shouldBe "*"
  }

  test("divide with Any literal") {
    val c = Column("x").divide(5)
    c.expr.getUnresolvedFunction.getFunctionName shouldBe "/"
  }

  test("mod with Any literal") {
    val c = Column("x").mod(5)
    c.expr.getUnresolvedFunction.getFunctionName shouldBe "%"
  }

  // ---------- Coverage boost: bitwise operators ----------

  test("bitwiseOR creates | function") {
    val c = Column("x").bitwiseOR(0xff)
    c.expr.getUnresolvedFunction.getFunctionName shouldBe "|"
    c.expr.getUnresolvedFunction.getArgumentsCount shouldBe 2
  }

  test("bitwiseAND creates & function") {
    val c = Column("x").bitwiseAND(0xff)
    c.expr.getUnresolvedFunction.getFunctionName shouldBe "&"
  }

  test("bitwiseXOR creates ^ function") {
    val c = Column("x").bitwiseXOR(0xff)
    c.expr.getUnresolvedFunction.getFunctionName shouldBe "^"
  }

  test("| is alias for bitwiseOR") {
    val c = Column("x") | 0xff
    c.expr.getUnresolvedFunction.getFunctionName shouldBe "|"
  }

  test("& is alias for bitwiseAND") {
    val c = Column("x") & 0xff
    c.expr.getUnresolvedFunction.getFunctionName shouldBe "&"
  }

  test("^ is alias for bitwiseXOR") {
    val c = Column("x") ^ 0xff
    c.expr.getUnresolvedFunction.getFunctionName shouldBe "^"
  }

  // ---------- Coverage boost: string operators with Column args ----------

  test("contains with Column argument") {
    val c = Column("x").contains(Column("y"))
    c.expr.getUnresolvedFunction.getFunctionName shouldBe "contains"
    c.expr.getUnresolvedFunction.getArgumentsCount shouldBe 2
  }

  test("startsWith with Column argument") {
    val c = Column("x").startsWith(Column("y"))
    c.expr.getUnresolvedFunction.getFunctionName shouldBe "startswith"
  }

  test("endsWith with Column argument") {
    val c = Column("x").endsWith(Column("y"))
    c.expr.getUnresolvedFunction.getFunctionName shouldBe "endswith"
  }

  // ---------- Coverage boost: between ----------

  test("between creates >= and <= combined with and") {
    val c = Column("x").between(1, 10)
    c.expr.hasUnresolvedFunction shouldBe true
    c.expr.getUnresolvedFunction.getFunctionName shouldBe "and"
    c.expr.getUnresolvedFunction.getArgumentsCount shouldBe 2
    // first arg is (x >= 1), second is (x <= 10)
    val args = c.expr.getUnresolvedFunction.getArgumentsList
    args.get(0).getUnresolvedFunction.getFunctionName shouldBe ">="
    args.get(1).getUnresolvedFunction.getFunctionName shouldBe "<="
  }

  // ---------- Coverage boost: substr ----------

  test("substr(Int, Int) creates substring function") {
    val c = Column("x").substr(1, 5)
    c.expr.hasUnresolvedFunction shouldBe true
    c.expr.getUnresolvedFunction.getFunctionName shouldBe "substring"
    c.expr.getUnresolvedFunction.getArgumentsCount shouldBe 3
  }

  test("substr(Column, Column) creates substring function") {
    val c = Column("x").substr(Column.lit(1), Column.lit(5))
    c.expr.getUnresolvedFunction.getFunctionName shouldBe "substring"
    c.expr.getUnresolvedFunction.getArgumentsCount shouldBe 3
  }

  // ---------- Coverage boost: isInCollection ----------

  test("isInCollection delegates to isin") {
    val c = Column("x").isInCollection(Seq(1, 2, 3))
    c.expr.getUnresolvedFunction.getFunctionName shouldBe "in"
    c.expr.getUnresolvedFunction.getArgumentsCount shouldBe 4
  }

  // ---------- Coverage boost: isin with empty values ----------

  test("isin with no values creates 'in' function with just the column") {
    val c = Column("x").isin()
    c.expr.getUnresolvedFunction.getFunctionName shouldBe "in"
    c.expr.getUnresolvedFunction.getArgumentsCount shouldBe 1
  }

  // ---------- Coverage boost: isin with DataFrame (subquery) ----------

  test("isin(DataFrame) builds SubqueryExpression with IN type") {
    val session = SparkSession(null)
    import org.apache.spark.connect.proto.{
      Relation, RelationCommon, LocalRelation, SubqueryExpression
    }
    val rel = Relation.newBuilder()
      .setCommon(RelationCommon.newBuilder().setPlanId(session.nextPlanId()).build())
      .setLocalRelation(LocalRelation.getDefaultInstance)
      .build()
    val df = DataFrame(session, rel)
    val c = Column("id").isin(df)
    c.expr.hasSubqueryExpression shouldBe true
    val sub = c.expr.getSubqueryExpression
    sub.getSubqueryType shouldBe SubqueryExpression.SubqueryType.SUBQUERY_TYPE_IN
    sub.getInSubqueryValuesCount shouldBe 1
  }

  // ---------- Coverage boost: when / otherwise with Column value ----------

  test("when with Column value") {
    val c = functions.when(Column("x") > 0, Column.lit("positive"))
    c.expr.hasUnresolvedFunction shouldBe true
    c.expr.getUnresolvedFunction.getFunctionName shouldBe "when"
    c.expr.getUnresolvedFunction.getArgumentsCount shouldBe 2
  }

  test("otherwise with Column value") {
    val c = functions.when(Column("x") > 0, "positive")
      .otherwise(Column.lit("negative"))
    c.expr.getUnresolvedFunction.getFunctionName shouldBe "when"
    c.expr.getUnresolvedFunction.getArgumentsCount shouldBe 3
  }

  // ---------- Coverage boost: alias variants ----------

  test("alias is same as as(name)") {
    val c = Column("x").alias("renamed")
    c.expr.hasAlias shouldBe true
    c.expr.getAlias.getNameList.asScala.toSeq shouldBe Seq("renamed")
  }

  test("name is same as as(name)") {
    val c = Column("x").name("renamed")
    c.expr.hasAlias shouldBe true
    c.expr.getAlias.getNameList.asScala.toSeq shouldBe Seq("renamed")
  }

  test("as with multiple aliases") {
    val c = Column("x").as(Seq("a", "b", "c"))
    c.expr.hasAlias shouldBe true
    c.expr.getAlias.getNameList.asScala.toSeq shouldBe Seq("a", "b", "c")
  }

  test("as with alias and metadata") {
    val c = Column("x").as("renamed", """{"key":"value"}""")
    c.expr.hasAlias shouldBe true
    val alias = c.expr.getAlias
    alias.getNameList.asScala.toSeq shouldBe Seq("renamed")
    alias.getMetadata shouldBe """{"key":"value"}"""
  }

  // ---------- Coverage boost: sort order variants ----------

  test("asc_nulls_first creates ascending with nulls first") {
    val c = Column("x").asc_nulls_first
    c.expr.hasSortOrder shouldBe true
    val so = c.expr.getSortOrder
    so.getDirection shouldBe Expression.SortOrder.SortDirection.SORT_DIRECTION_ASCENDING
    so.getNullOrdering shouldBe Expression.SortOrder.NullOrdering.SORT_NULLS_FIRST
  }

  test("asc_nulls_last creates ascending with nulls last") {
    val c = Column("x").asc_nulls_last
    c.expr.hasSortOrder shouldBe true
    val so = c.expr.getSortOrder
    so.getDirection shouldBe Expression.SortOrder.SortDirection.SORT_DIRECTION_ASCENDING
    so.getNullOrdering shouldBe Expression.SortOrder.NullOrdering.SORT_NULLS_LAST
  }

  test("desc_nulls_first creates descending with nulls first") {
    val c = Column("x").desc_nulls_first
    c.expr.hasSortOrder shouldBe true
    val so = c.expr.getSortOrder
    so.getDirection shouldBe Expression.SortOrder.SortDirection.SORT_DIRECTION_DESCENDING
    so.getNullOrdering shouldBe Expression.SortOrder.NullOrdering.SORT_NULLS_FIRST
  }

  test("desc_nulls_last creates descending with nulls last") {
    val c = Column("x").desc_nulls_last
    c.expr.hasSortOrder shouldBe true
    val so = c.expr.getSortOrder
    so.getDirection shouldBe Expression.SortOrder.SortDirection.SORT_DIRECTION_DESCENDING
    so.getNullOrdering shouldBe Expression.SortOrder.NullOrdering.SORT_NULLS_LAST
  }

  test("asc default has nulls first") {
    val so = Column("x").asc.expr.getSortOrder
    so.getDirection shouldBe Expression.SortOrder.SortDirection.SORT_DIRECTION_ASCENDING
    so.getNullOrdering shouldBe Expression.SortOrder.NullOrdering.SORT_NULLS_FIRST
  }

  test("desc default has nulls last") {
    val so = Column("x").desc.expr.getSortOrder
    so.getDirection shouldBe Expression.SortOrder.SortDirection.SORT_DIRECTION_DESCENDING
    so.getNullOrdering shouldBe Expression.SortOrder.NullOrdering.SORT_NULLS_LAST
  }

  // ---------- Coverage boost: toSortOrder ----------

  test("toSortOrder on non-SortOrder column creates ascending default") {
    val c = Column("x")
    val so = c.toSortOrder
    so.getDirection shouldBe Expression.SortOrder.SortDirection.SORT_DIRECTION_ASCENDING
    so.getNullOrdering shouldBe Expression.SortOrder.NullOrdering.SORT_NULLS_FIRST
    so.getChild shouldBe c.expr
  }

  test("toSortOrder on SortOrder column returns the existing SortOrder") {
    val c = Column("x").desc_nulls_first
    val so = c.toSortOrder
    so.getDirection shouldBe Expression.SortOrder.SortDirection.SORT_DIRECTION_DESCENDING
    so.getNullOrdering shouldBe Expression.SortOrder.NullOrdering.SORT_NULLS_FIRST
  }

  // ---------- Coverage boost: over (window) ----------

  test("over(WindowSpec) creates Window expression") {
    val ws = Window.partitionBy(Column("dept")).orderBy(Column("salary"))
    val c = Column("x").over(ws)
    c.expr.hasWindow shouldBe true
    val w = c.expr.getWindow
    w.getPartitionSpecCount shouldBe 1
    w.getOrderSpecCount shouldBe 1
    w.hasFrameSpec shouldBe false
  }

  test("over() with empty WindowSpec") {
    val c = Column("x").over()
    c.expr.hasWindow shouldBe true
    val w = c.expr.getWindow
    w.getPartitionSpecCount shouldBe 0
    w.getOrderSpecCount shouldBe 0
  }

  test("over with frame spec") {
    val ws = Window.partitionBy(Column("dept"))
      .orderBy(Column("salary"))
      .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    val c = Column("x").over(ws)
    c.expr.hasWindow shouldBe true
    c.expr.getWindow.hasFrameSpec shouldBe true
  }

  // ---------- Coverage boost: dropFields single field ----------

  test("dropFields with single field creates UpdateFields without nesting") {
    val c = Column("s").dropFields("a")
    c.expr.hasUpdateFields shouldBe true
    val uf = c.expr.getUpdateFields
    uf.getFieldName shouldBe "a"
    uf.hasValueExpression shouldBe false
    // struct expression should be the original column
    uf.getStructExpression.hasUnresolvedAttribute shouldBe true
  }

  // ---------- Coverage boost: toString ----------

  test("toString returns the expression's string form") {
    val c = Column("x")
    c.toString should include("unparsed_identifier")
  }

  // ---------- Coverage boost: getItem with integer key ----------

  test("getItem with integer key creates UnresolvedExtractValue") {
    val c = Column("arr").getItem(0)
    c.expr.hasUnresolvedExtractValue shouldBe true
    val ev = c.expr.getUnresolvedExtractValue
    ev.getExtraction.hasLiteral shouldBe true
    ev.getExtraction.getLiteral.hasInteger shouldBe true
    ev.getExtraction.getLiteral.getInteger shouldBe 0
  }

  // ---------- Coverage boost: getField extraction details ----------

  test("getField extraction is a string literal") {
    val c = Column("s").getField("name")
    val ev = c.expr.getUnresolvedExtractValue
    ev.getExtraction.hasLiteral shouldBe true
    ev.getExtraction.getLiteral.hasString shouldBe true
    ev.getExtraction.getLiteral.getString shouldBe "name"
  }

  // ---------- Coverage boost: withField value expression details ----------

  test("withField value expression is the provided column's expr") {
    val valCol = Column.lit("hello")
    val c = Column("s").withField("greeting", valCol)
    c.expr.hasUpdateFields shouldBe true
    val uf = c.expr.getUpdateFields
    uf.getValueExpression shouldBe valCol.expr
    uf.getStructExpression shouldBe Column("s").expr
  }

  // ---------- Coverage boost: cast with DataType verifies roundtrip ----------

  test("cast(DataType) preserves the type in proto") {
    val c = Column("a").cast(types.StringType)
    val cast = c.expr.getCast
    cast.hasType shouldBe true
    // The inner expr should be the original column
    cast.getExpr.hasUnresolvedAttribute shouldBe true
    cast.getExpr.getUnresolvedAttribute.getUnparsedIdentifier shouldBe "a"
  }

  // ---------- Coverage boost: like/rlike argument details ----------

  test("like passes the literal pattern as second argument") {
    val c = Column("name").like("%test%")
    val fn = c.expr.getUnresolvedFunction
    fn.getArgumentsCount shouldBe 2
    fn.getArguments(1).getLiteral.getString shouldBe "%test%"
  }

  test("rlike passes the pattern as second argument") {
    val c = Column("name").rlike("^test")
    val fn = c.expr.getUnresolvedFunction
    fn.getArguments(1).getLiteral.getString shouldBe "^test"
  }

  test("ilike passes the pattern as second argument") {
    val c = Column("name").ilike("%Hello%")
    val fn = c.expr.getUnresolvedFunction
    fn.getArguments(1).getLiteral.getString shouldBe "%Hello%"
  }

  // ---------- Coverage boost: isin verifies literal values ----------

  test("isin literal values are correct") {
    val c = Column("x").isin("a", "b")
    val fn = c.expr.getUnresolvedFunction
    fn.getArguments(0).hasUnresolvedAttribute shouldBe true // the column itself
    fn.getArguments(1).getLiteral.getString shouldBe "a"
    fn.getArguments(2).getLiteral.getString shouldBe "b"
  }

  // ---------- Coverage boost: when/otherwise argument structure ----------

  test("functions.when creates initial when with 2 arguments") {
    val c = functions.when(Column("x") > 0, "pos")
    c.expr.hasUnresolvedFunction shouldBe true
    val fn = c.expr.getUnresolvedFunction
    fn.getFunctionName shouldBe "when"
    fn.getArgumentsCount shouldBe 2
  }

  test("chained when adds 2 more arguments each time") {
    val c = functions.when(Column("x") > 0, "pos")
      .when(Column("x") < 0, "neg")
    val fn = c.expr.getUnresolvedFunction
    fn.getArgumentsCount shouldBe 4
  }

  test("otherwise adds 1 more argument") {
    val c = functions.when(Column("x") > 0, "pos")
      .otherwise("other")
    val fn = c.expr.getUnresolvedFunction
    fn.getArgumentsCount shouldBe 3
  }
