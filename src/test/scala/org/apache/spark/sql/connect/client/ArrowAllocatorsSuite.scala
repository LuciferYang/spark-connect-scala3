package org.apache.spark.sql.connect.client

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class ArrowAllocatorsSuite extends AnyFunSuite with Matchers:

  test("resolve falls back to the default when unset, invalid, or non-positive") {
    ArrowAllocators.resolve(None) shouldBe ArrowAllocators.DefaultMaxBytes
    ArrowAllocators.resolve(Some("not-a-number")) shouldBe ArrowAllocators.DefaultMaxBytes
    ArrowAllocators.resolve(Some("0")) shouldBe ArrowAllocators.DefaultMaxBytes
    ArrowAllocators.resolve(Some("-5")) shouldBe ArrowAllocators.DefaultMaxBytes
  }

  test("resolve honors a valid positive value (trimmed)") {
    ArrowAllocators.resolve(Some("1048576")) shouldBe 1048576L
    ArrowAllocators.resolve(Some(" 2048 ")) shouldBe 2048L
  }
