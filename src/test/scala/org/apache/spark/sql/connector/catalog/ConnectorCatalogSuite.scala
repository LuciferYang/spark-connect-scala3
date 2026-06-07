package org.apache.spark.sql.connector.catalog

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class ConnectorCatalogSuite extends AnyFunSuite with Matchers:

  test("Identifier.of creates identifiers with namespace and name") {
    val identifier = Identifier.of(Array("catalog", "db"), "table")

    identifier.namespace().toSeq shouldBe Seq("catalog", "db")
    identifier.name() shouldBe "table"
    identifier.toString shouldBe "catalog.db.table"
  }

  test("Identifier.toString quotes unsafe parts") {
    val identifier = Identifier.of(Array("my catalog", "db`x"), "table-name")

    identifier.toString shouldBe "`my catalog`.`db``x`.`table-name`"
  }

  test("IdentityColumnSpec exposes value semantics") {
    val spec = new IdentityColumnSpec(1L, 2L, true)
    val same = new IdentityColumnSpec(1L, 2L, true)

    spec.getStart shouldBe 1L
    spec.getStep shouldBe 2L
    spec.isAllowExplicitInsert shouldBe true
    spec shouldBe same
    spec.hashCode shouldBe same.hashCode
  }
