package org.apache.spark.sql.connect.common

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/** Tests for UdfAdaptors — top-level serializable function wrappers. */
class UdfAdaptorsSuite extends AnyFunSuite with Matchers:

  // ---------------------------------------------------------------------------
  // MapPartitionsAdaptor
  // ---------------------------------------------------------------------------

  test("MapPartitionsAdaptor maps elements") {
    val adaptor = MapPartitionsAdaptor[Int, String](_.toString)
    val result = adaptor(Iterator(1, 2, 3)).toList
    result shouldBe List("1", "2", "3")
  }

  test("MapPartitionsAdaptor with empty iterator") {
    val adaptor = MapPartitionsAdaptor[Int, Int](_ * 2)
    adaptor(Iterator.empty).toList shouldBe empty
  }

  // ---------------------------------------------------------------------------
  // FilterAdaptor
  // ---------------------------------------------------------------------------

  test("FilterAdaptor filters elements") {
    val adaptor = FilterAdaptor[Int](_ > 2)
    val result = adaptor(Iterator(1, 2, 3, 4)).toList
    result shouldBe List(3, 4)
  }

  test("FilterAdaptor with no matches") {
    val adaptor = FilterAdaptor[Int](_ > 100)
    adaptor(Iterator(1, 2, 3)).toList shouldBe empty
  }

  // ---------------------------------------------------------------------------
  // FlatMapAdaptor
  // ---------------------------------------------------------------------------

  test("FlatMapAdaptor flatmaps elements") {
    val adaptor = FlatMapAdaptor[Int, Int](n => List(n, n * 10))
    val result = adaptor(Iterator(1, 2)).toList
    result shouldBe List(1, 10, 2, 20)
  }

  test("FlatMapAdaptor with empty results") {
    val adaptor = FlatMapAdaptor[Int, Int](_ => Nil)
    adaptor(Iterator(1, 2, 3)).toList shouldBe empty
  }

  // ---------------------------------------------------------------------------
  // MapGroupsAdaptor
  // ---------------------------------------------------------------------------

  test("MapGroupsAdaptor wraps result in Iterator.single") {
    val adaptor = MapGroupsAdaptor[String, Int, Int]((_, iter) => iter.sum)
    val result = adaptor("key", Iterator(1, 2, 3))
    result.iterator.toList shouldBe List(6)
  }

  // ---------------------------------------------------------------------------
  // CountGroupsAdaptor
  // ---------------------------------------------------------------------------

  test("CountGroupsAdaptor counts elements") {
    val adaptor = CountGroupsAdaptor[String]()
    val result = adaptor("key", Iterator(1, 2, 3))
    result shouldBe ("key", 3L)
  }

  test("CountGroupsAdaptor with empty group") {
    val adaptor = CountGroupsAdaptor[String]()
    adaptor("k", Iterator.empty) shouldBe ("k", 0L)
  }

  // ---------------------------------------------------------------------------
  // ReduceGroupsAdaptor
  // ---------------------------------------------------------------------------

  test("ReduceGroupsAdaptor reduces values") {
    val adaptor = ReduceGroupsAdaptor[String, Int](_ + _)
    val result = adaptor("key", Iterator(1, 2, 3))
    result shouldBe ("key", 6)
  }

  test("ReduceGroupsAdaptor throws NoSuchElementException on empty iterator") {
    val adaptor = ReduceGroupsAdaptor[String, Int](_ + _)
    val ex = intercept[NoSuchElementException] {
      adaptor("emptyKey", Iterator.empty)
    }
    ex.getMessage should include("emptyKey")
  }

  test("ReduceGroupsAdaptor with single element") {
    val adaptor = ReduceGroupsAdaptor[String, Int](_ + _)
    val result = adaptor("k", Iterator(42))
    result shouldBe ("k", 42)
  }

  // ---------------------------------------------------------------------------
  // MapValuesFlatMapAdaptor
  // ---------------------------------------------------------------------------

  test("MapValuesFlatMapAdaptor applies value transform before flatMap") {
    val adaptor = MapValuesFlatMapAdaptor[String, String](
      valueMapFunc = (v: Any) => v.asInstanceOf[Int] * 2,
      flatMapFunc = (k, iter) => iter.map(v => s"$k:$v")
    )
    val result = adaptor("a", Iterator(1, 2, 3)).iterator.toList
    result shouldBe List("a:2", "a:4", "a:6")
  }

  // ---------------------------------------------------------------------------
  // CoGroupAdaptor
  // ---------------------------------------------------------------------------

  test("CoGroupAdaptor delegates to function") {
    val adaptor =
      CoGroupAdaptor[String, Int, Int, Int]((_, left, right) => Iterator(left.sum + right.sum))
    val result = adaptor("k", Iterator(1, 2), Iterator(3, 4)).iterator.toList
    result shouldBe List(10)
  }

  // ---------------------------------------------------------------------------
  // MapValuesCoGroupAdaptor
  // ---------------------------------------------------------------------------

  test("MapValuesCoGroupAdaptor passes through when neither side has mapValues") {
    val adaptor = MapValuesCoGroupAdaptor[String, Int, Int, Int](
      leftValueMapFunc = None,
      rightValueMapFunc = None,
      func = (_, l, r) => Iterator(l.sum + r.sum)
    )
    val result = adaptor("k", Iterator(1, 2, 3), Iterator(10, 20)).iterator.toList
    result shouldBe List(36)
  }

  test("MapValuesCoGroupAdaptor applies left transform only") {
    val adaptor = MapValuesCoGroupAdaptor[String, Int, Int, Int](
      leftValueMapFunc = Some((v: Any) => v.asInstanceOf[Int] * 10),
      rightValueMapFunc = None,
      func = (_, l, r) => Iterator(l.sum + r.sum)
    )
    val result = adaptor("k", Iterator(1, 2), Iterator(100, 200)).iterator.toList
    // left: 1*10 + 2*10 = 30; right: 100 + 200 = 300; total 330
    result shouldBe List(330)
  }

  test("MapValuesCoGroupAdaptor applies right transform only") {
    val adaptor = MapValuesCoGroupAdaptor[String, Int, Int, Int](
      leftValueMapFunc = None,
      rightValueMapFunc = Some((v: Any) => v.asInstanceOf[Int] + 1),
      func = (_, l, r) => Iterator(l.sum + r.sum)
    )
    val result = adaptor("k", Iterator(1, 2), Iterator(10, 20)).iterator.toList
    // left: 1+2=3; right: 11+21=32; total 35
    result shouldBe List(35)
  }

  test("MapValuesCoGroupAdaptor applies transforms on both sides") {
    val adaptor = MapValuesCoGroupAdaptor[String, String, String, String](
      leftValueMapFunc = Some((v: Any) => s"L${v}"),
      rightValueMapFunc = Some((v: Any) => s"R${v}"),
      func = (k, l, r) => Iterator(s"$k=${l.toList.mkString(",")}|${r.toList.mkString(",")}")
    )
    val result = adaptor("g", Iterator(1, 2), Iterator(9, 8)).iterator.toList
    result shouldBe List("g=L1,L2|R9,R8")
  }

  // ---------------------------------------------------------------------------
  // Serializable
  // ---------------------------------------------------------------------------

  test("all adaptors are Serializable") {
    import java.io.{ByteArrayOutputStream, ObjectOutputStream}

    val adaptors: Seq[Serializable] = Seq(
      MapPartitionsAdaptor[Int, Int](_ + 1),
      FilterAdaptor[Int](_ > 0),
      FlatMapAdaptor[Int, Int](n => List(n)),
      MapGroupsAdaptor[String, Int, Int]((_, iter) => iter.sum),
      CountGroupsAdaptor[String](),
      ReduceGroupsAdaptor[String, Int](_ + _),
      MapValuesFlatMapAdaptor[String, Int](
        (v: Any) => v,
        (_, iter) => iter.map(_.asInstanceOf[Int])
      ),
      CoGroupAdaptor[String, Int, Int, Int]((_, l, r) => Iterator.empty),
      MapValuesCoGroupAdaptor[String, Int, Int, Int](
        Some((v: Any) => v.asInstanceOf[Int]),
        Some((v: Any) => v.asInstanceOf[Int]),
        (_, l, r) => Iterator.empty
      )
    )

    for adaptor <- adaptors do
      val bos = ByteArrayOutputStream()
      val oos = ObjectOutputStream(bos)
      noException should be thrownBy oos.writeObject(adaptor)
      oos.close()
      bos.toByteArray.length should be > 0
  }
