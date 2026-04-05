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
