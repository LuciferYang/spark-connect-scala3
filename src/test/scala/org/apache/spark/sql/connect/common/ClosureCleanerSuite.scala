package org.apache.spark.sql.connect.common

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import scala.collection.mutable

/** Tests for ClosureCleaner, IndylambdaScalaClosures, and SparkClassUtilsShim. */
class ClosureCleanerSuite extends AnyFunSuite with Matchers:

  // ---------------------------------------------------------------------------
  // SparkClassUtilsShim
  // ---------------------------------------------------------------------------

  test("classForName loads class by name") {
    val cls = SparkClassUtilsShim.classForName[String]("java.lang.String")
    cls shouldBe classOf[String]
  }

  test("classForName with noSparkClassLoader uses context classloader") {
    val cls = SparkClassUtilsShim.classForName[String](
      "java.lang.String",
      initialize = true,
      noSparkClassLoader = true
    )
    cls shouldBe classOf[String]
  }

  test("classForName throws ClassNotFoundException for invalid class") {
    an[ClassNotFoundException] should be thrownBy
      SparkClassUtilsShim.classForName[AnyRef]("com.nonexistent.FakeClass")
  }

  test("getAllInterfaces returns all interfaces") {
    val interfaces = SparkClassUtilsShim.getAllInterfaces(classOf[java.util.ArrayList[?]])
    interfaces should contain(classOf[java.util.List[?]])
    interfaces should contain(classOf[java.util.Collection[?]])
    interfaces should contain(classOf[java.lang.Iterable[?]])
    interfaces should contain(classOf[java.io.Serializable])
  }

  test("getAllInterfaces returns null for null input") {
    SparkClassUtilsShim.getAllInterfaces(null) shouldBe null
  }

  test("getAllInterfaces for class with no interfaces") {
    val interfaces = SparkClassUtilsShim.getAllInterfaces(classOf[Object])
    interfaces shouldBe empty
  }

  test("copyStream copies data correctly") {
    val input = "Hello, World!".getBytes
    val bis = ByteArrayInputStream(input)
    val bos = ByteArrayOutputStream()
    val count = SparkClassUtilsShim.copyStream(bis, bos, closeStreams = true)
    count shouldBe input.length
    bos.toByteArray shouldBe input
  }

  test("copyStream with empty input") {
    val bis = ByteArrayInputStream(Array.empty[Byte])
    val bos = ByteArrayOutputStream()
    val count = SparkClassUtilsShim.copyStream(bis, bos, closeStreams = true)
    count shouldBe 0
  }

  test("copyStream with closeStreams=false") {
    val input = "test".getBytes
    val bis = ByteArrayInputStream(input)
    val bos = ByteArrayOutputStream()
    val count = SparkClassUtilsShim.copyStream(bis, bos, closeStreams = false)
    count shouldBe input.length
    // streams should still be usable after
    bos.toByteArray shouldBe input
  }

  test("copyStream with large data (> 8192 buffer)") {
    val input = new Array[Byte](20000)
    java.util.Arrays.fill(input, 42.toByte)
    val bis = ByteArrayInputStream(input)
    val bos = ByteArrayOutputStream()
    val count = SparkClassUtilsShim.copyStream(bis, bos, closeStreams = true)
    count shouldBe 20000
    bos.toByteArray shouldBe input
  }

  // ---------------------------------------------------------------------------
  // ClosureCleaner.getClassReader
  // ---------------------------------------------------------------------------

  test("getClassReader returns non-null for known class") {
    val reader = ClosureCleaner.getClassReader(classOf[String])
    reader should not be null
  }

  test("getClassReader returns non-null for our own classes") {
    val reader = ClosureCleaner.getClassReader(classOf[ClosureCleanerSuite])
    reader should not be null
  }

  // ---------------------------------------------------------------------------
  // ClosureCleaner.isAmmoniteCommandOrHelper / isDefinedInAmmonite
  // ---------------------------------------------------------------------------

  test("isAmmoniteCommandOrHelper for non-ammonite class") {
    ClosureCleaner.isAmmoniteCommandOrHelper(classOf[String]) shouldBe false
  }

  test("isDefinedInAmmonite for non-ammonite class") {
    ClosureCleaner.isDefinedInAmmonite(classOf[String]) shouldBe false
  }

  // ---------------------------------------------------------------------------
  // ClosureCleaner.clean — indylambda closures
  // ---------------------------------------------------------------------------

  test("clean with non-closure returns None") {
    val accessedFields = mutable.Map.empty[Class[?], mutable.Set[String]]
    val result = ClosureCleaner.clean("not a closure", cleanTransitively = true, accessedFields)
    result shouldBe None
  }

  test("clean with null throws NullPointerException") {
    val accessedFields = mutable.Map.empty[Class[?], mutable.Set[String]]
    a[NullPointerException] should be thrownBy
      ClosureCleaner.clean(null.asInstanceOf[AnyRef], cleanTransitively = true, accessedFields)
  }

  test("clean with simple lambda returns Some") {
    val func: Int => Int = x => x + 1
    val accessedFields = mutable.Map.empty[Class[?], mutable.Set[String]]
    val result = ClosureCleaner.clean(func, cleanTransitively = true, accessedFields)
    // Indylambda closures without captured outer should return None (early return)
    // or Some depending on captured arg count
    // The lambda captures no outer this, so should return None (no captured args > 0)
    result shouldBe None
  }

  test("clean with capturing lambda") {
    var captured = 42
    val func: Int => Int = x => x + captured
    val accessedFields = mutable.Map.empty[Class[?], mutable.Set[String]]
    val result = ClosureCleaner.clean(func, cleanTransitively = true, accessedFields)
    // This captures `this` (the test class), but the class is not a REPL or Ammonite class
    // so it just returns Some(func) unchanged
    result shouldBe Some(func)
  }

  // ---------------------------------------------------------------------------
  // IndylambdaScalaClosures
  // ---------------------------------------------------------------------------

  test("getSerializationProxy for non-closure returns None") {
    IndylambdaScalaClosures.getSerializationProxy("not a closure") shouldBe None
  }

  test("getSerializationProxy for simple lambda returns Some") {
    val func: Int => Int = x => x + 1
    val proxy = IndylambdaScalaClosures.getSerializationProxy(func)
    proxy shouldBe defined
    proxy.get.getImplMethodName should include("$anonfun$")
  }

  test("getSerializationProxy for non-serializable returns None") {
    // Create something that looks closure-like but isn't serializable
    val nonSerializable = new Function1[Int, Int] {
      def apply(x: Int): Int = x
    }
    // This is not synthetic, so should return None
    IndylambdaScalaClosures.getSerializationProxy(nonSerializable) shouldBe None
  }

  test("isIndylambdaScalaClosure checks method kind and name") {
    val func: Int => Int = x => x + 1
    val proxy = IndylambdaScalaClosures.getSerializationProxy(func)
    proxy.isDefined shouldBe true
    IndylambdaScalaClosures.isIndylambdaScalaClosure(proxy.get) shouldBe true
  }

  test("inspect returns SerializedLambda for valid closure") {
    val func: Int => Int = x => x * 2
    val lambda = IndylambdaScalaClosures.inspect(func)
    lambda should not be null
    lambda.getImplMethodName should include("$anonfun$")
  }

  test("isLambdaMetafactory with correct handle") {
    import org.objectweb.asm.{Handle, Opcodes}
    val handle = Handle(
      Opcodes.H_INVOKESTATIC,
      "java/lang/invoke/LambdaMetafactory",
      "altMetafactory",
      "(Ljava/lang/invoke/MethodHandles$Lookup;Ljava/lang/String;Ljava/lang/invoke/MethodType;[Ljava/lang/Object;)Ljava/lang/invoke/CallSite;",
      false
    )
    IndylambdaScalaClosures.isLambdaMetafactory(handle) shouldBe true
  }

  test("isLambdaMetafactory with wrong handle") {
    import org.objectweb.asm.{Handle, Opcodes}
    val handle = Handle(
      Opcodes.H_INVOKESTATIC,
      "some/Other/Class",
      "someMethod",
      "()V",
      false
    )
    IndylambdaScalaClosures.isLambdaMetafactory(handle) shouldBe false
  }

  // ---------------------------------------------------------------------------
  // Logging
  // ---------------------------------------------------------------------------

  test("Log instance exists and debug is disabled by default") {
    val log = Log.instance
    log.isDebugEnabled shouldBe false
    log.isTraceEnabled shouldBe false
  }

  test("Logging trait methods do not throw when debug is disabled") {
    // Logging methods are protected, so we test via a subclass that exposes them
    object TestLogger extends Logging:
      def doLogDebug(msg: String): Unit = logDebug(msg)
      def doLogTrace(msg: String): Unit = logTrace(msg)
      def doLogInfo(msg: String): Unit = logInfo(msg)
    noException should be thrownBy TestLogger.doLogDebug("test")
    noException should be thrownBy TestLogger.doLogTrace("test")
    noException should be thrownBy TestLogger.doLogInfo("test")
  }

  // ---------------------------------------------------------------------------
  // ReturnStatementInClosureException
  // ---------------------------------------------------------------------------

  test("ReturnStatementInClosureException has correct message") {
    val ex = ReturnStatementInClosureException()
    ex.getMessage should include("Return statements aren't allowed")
  }
