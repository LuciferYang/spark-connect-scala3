package org.apache.spark.sql.types

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}

class VariantValSuite extends AnyFunSuite with Matchers:

  test("VariantVal stores value and metadata") {
    val v = VariantVal(Array[Byte](1, 2, 3), Array[Byte](10, 20))
    v.getValue shouldBe Array[Byte](1, 2, 3)
    v.getMetadata shouldBe Array[Byte](10, 20)
  }

  test("VariantVal equals compares by content") {
    val v1 = VariantVal(Array[Byte](1, 2), Array[Byte](3))
    val v2 = VariantVal(Array[Byte](1, 2), Array[Byte](3))
    val v3 = VariantVal(Array[Byte](1, 2), Array[Byte](4))
    val v4 = VariantVal(Array[Byte](9, 2), Array[Byte](3))

    v1 shouldBe v2
    v1 should not be v3
    v1 should not be v4
  }

  test("VariantVal hashCode is consistent with equals") {
    val v1 = VariantVal(Array[Byte](5, 6), Array[Byte](7, 8))
    val v2 = VariantVal(Array[Byte](5, 6), Array[Byte](7, 8))
    v1.hashCode() shouldBe v2.hashCode()
  }

  test("VariantVal equals returns false for non-VariantVal") {
    val v = VariantVal(Array[Byte](1), Array[Byte](2))
    v.equals("not a variant") shouldBe false
    v.equals(null) shouldBe false
  }

  test("VariantVal debugString contains byte arrays") {
    val v = VariantVal(Array[Byte](1, 2), Array[Byte](3))
    v.debugString should include("VariantVal")
    v.debugString should include("1, 2")
    v.debugString should include("3")
  }

  test("VariantVal toString returns debugString") {
    val v = VariantVal(Array[Byte](1), Array[Byte](2))
    v.toString shouldBe v.debugString
  }

  test("VariantVal is Serializable") {
    val original = VariantVal(Array[Byte](1, 2, 3, 4), Array[Byte](10, 20, 30))
    val bos = ByteArrayOutputStream()
    val oos = ObjectOutputStream(bos)
    oos.writeObject(original)
    oos.close()

    val bis = ByteArrayInputStream(bos.toByteArray)
    val ois = ObjectInputStream(bis)
    val restored = ois.readObject().asInstanceOf[VariantVal]
    ois.close()

    restored shouldBe original
    restored.getValue shouldBe original.getValue
    restored.getMetadata shouldBe original.getMetadata
  }

  test("VariantVal with empty arrays") {
    val v = VariantVal(Array.empty[Byte], Array.empty[Byte])
    v.getValue shouldBe empty
    v.getMetadata shouldBe empty
    v.debugString should include("VariantVal")
  }
