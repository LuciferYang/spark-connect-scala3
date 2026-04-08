package org.apache.spark.sql.util

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}
import java.lang.invoke.SerializedLambda
import java.lang.reflect.Field

/** Simplified closure cleaner for Scala 3 Spark Connect client.
  *
  * The upstream Spark `ClosureCleaner` uses ASM bytecode analysis to determine which captured
  * fields a lambda actually uses, then nulls out unused fields on cloned outer objects. This is
  * essential for making closures serializable when they capture large, non-serializable outer
  * scopes (e.g., SparkContext).
  *
  * This simplified version for Scala 3 focuses on the indylambda path (the only path Scala 3
  * uses) and performs basic outer-reference cleaning without ASM. It:
  *
  * 1. Detects if the closure is an indylambda (via `SerializedLambda`)
  * 2. Nulls out `$outer` references in captured objects when they are not serializable
  * 3. Verifies the cleaned closure is serializable
  */
object ClosureCleaner:

  /** Clean a closure in-place to make it serializable.
    *
    * @param func
    *   The closure to clean
    * @param cleanTransitively
    *   Whether to clean outer references transitively
    */
  def clean(func: AnyRef, cleanTransitively: Boolean = true): Unit =
    if func == null then return
    // Only handle indylambda closures (the only kind Scala 3 produces)
    getSerializedLambda(func) match
      case Some(sl) => cleanIndyLambda(func, sl, cleanTransitively)
      case None     => // Not an indylambda; attempt to clean $outer fields directly
        cleanOuterFields(func)

  /** Verify that a closure is serializable by round-tripping through Java serialization. */
  def checkSerializable(func: AnyRef): Unit =
    val bytes = serialize(func)
    // We don't need to actually deserialize on the client side;
    // just verify serialization doesn't throw.
    if bytes.isEmpty then throw SparkSerializationException("Closure serialization produced empty bytes")

  /** Try to get the SerializedLambda for an indylambda closure. */
  private def getSerializedLambda(func: AnyRef): Option[SerializedLambda] =
    // Scala 3 (and Scala 2.12+) lambdas are generated via invokedynamic/LambdaMetafactory.
    // They implement Serializable and have a writeReplace() method that returns SerializedLambda.
    if !func.isInstanceOf[java.io.Serializable] then return None
    try
      val writeReplace = func.getClass.getDeclaredMethod("writeReplace")
      writeReplace.setAccessible(true)
      writeReplace.invoke(func) match
        case sl: SerializedLambda => Some(sl)
        case _                    => None
    catch
      case _: NoSuchMethodException => None
      case _: Exception             => None

  /** Clean an indylambda closure by examining captured args. */
  private def cleanIndyLambda(
      func: AnyRef,
      sl: SerializedLambda,
      cleanTransitively: Boolean
  ): Unit =
    // The SerializedLambda captures arguments (closed-over variables).
    // For each captured arg that is an object with $outer fields, clean those.
    val capturedArgCount = sl.getCapturedArgCount
    for i <- 0 until capturedArgCount do
      val arg = sl.getCapturedArg(i)
      if arg != null && !isPrimitiveOrWrapper(arg) then
        if cleanTransitively then cleanOuterFields(arg)

  /** Null out $outer fields on an object if they reference non-serializable instances. */
  private def cleanOuterFields(obj: AnyRef): Unit =
    if obj == null then return
    var clazz: Class[?] = obj.getClass
    while clazz != null do
      for field <- clazz.getDeclaredFields do
        if field.getName.contains("$outer") || field.getName == "arg$1" then
          field.setAccessible(true)
          try
            val value = field.get(obj)
            if value != null && !isSerializable(value) then field.set(obj, null)
          catch case _: Exception => () // ignore
      clazz = clazz.getSuperclass

  /** Check if a value is a primitive type or a boxed primitive. */
  private def isPrimitiveOrWrapper(obj: AnyRef): Boolean =
    obj match
      case _: java.lang.Boolean   => true
      case _: java.lang.Byte      => true
      case _: java.lang.Short     => true
      case _: java.lang.Integer   => true
      case _: java.lang.Long      => true
      case _: java.lang.Float     => true
      case _: java.lang.Double    => true
      case _: java.lang.Character => true
      case _: String              => true
      case _                      => false

  /** Check if an object is serializable (without actually serializing). */
  private def isSerializable(obj: AnyRef): Boolean =
    obj.isInstanceOf[java.io.Serializable]

  /** Serialize an object to bytes. */
  private def serialize(obj: AnyRef): Array[Byte] =
    val bos = ByteArrayOutputStream()
    val oos = ObjectOutputStream(bos)
    try
      oos.writeObject(obj)
      oos.flush()
      bos.toByteArray
    finally
      oos.close()
      bos.close()

/** Exception for serialization failures in closure cleaning. */
class SparkSerializationException(message: String, cause: Throwable = null)
    extends RuntimeException(message, cause)
