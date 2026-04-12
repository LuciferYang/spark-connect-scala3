/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.connect.common

// SC3 PORT NOTE
// =============
// This file is a near-verbatim port of `org.apache.spark.util.ClosureCleaner` from
// Apache Spark (common/utils, Apache-2.0). The original cleaner walks a Scala closure's
// bytecode via ASM, identifies unused outer-class references, and either nulls them out
// or rebuilds a clean lambda — making the closure safely round-trip across an
// `ObjectOutputStream` / `ObjectInputStream` pair (Spark Connect's UDF wire format).
//
// SC3 substitutions versus upstream:
//   - Package: `org.apache.spark.util` → `org.apache.spark.sql.connect.common`
//   - ASM: `org.apache.xbean.asm9.*` → `org.objectweb.asm.*` (stock ASM 9.x)
//   - Logging: `org.apache.spark.internal.Logging` → local stub in `Logging.scala`
//   - SparkException: `org.apache.spark.SparkException` → `org.apache.spark.sql.SparkException`
//   - SparkClassUtils: inlined as `private object SparkClassUtilsShim` below
//   - SparkStreamUtils: inlined `copyStream` below (8KB byte buffer loop)
//
// The algorithm itself is unmodified, so behavior should match upstream for both
// indylambda (Scala 2.12+, including Scala 3) and legacy non-indylambda closures.

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, InputStream, OutputStream}
import java.lang.invoke.{
  LambdaMetafactory, MethodHandle, MethodHandleInfo, MethodHandles, MethodType, SerializedLambda
}
import java.lang.reflect.{Field, Modifier}

import scala.annotation.nowarn
import scala.collection.mutable.{ArrayBuffer, LinkedHashSet, Map, Queue, Set, Stack}
import scala.jdk.CollectionConverters._

import org.objectweb.asm.{ClassReader, ClassVisitor, Handle, MethodVisitor, Opcodes, Type}
import org.objectweb.asm.tree.{ClassNode, MethodNode}

import org.apache.spark.sql.SparkException

/** Inlined replacement for the few `SparkClassUtils` and `SparkStreamUtils` helpers that the
  * upstream ClosureCleaner uses. Keeping these private avoids broadening SC3's public API.
  */
private[common] object SparkClassUtilsShim:
  def classForName[C](
      className: String,
      initialize: Boolean = true,
      noSparkClassLoader: Boolean = false
  ): Class[C] =
    val cl =
      if noSparkClassLoader then Thread.currentThread().getContextClassLoader
      else Option(Thread.currentThread().getContextClassLoader).getOrElse(getClass.getClassLoader)
    Class.forName(className, initialize, cl).asInstanceOf[Class[C]]

  def getAllInterfaces(cls: Class[?]): List[Class[?]] =
    if cls == null then return null
    val found = LinkedHashSet[Class[?]]()
    var currentClass: Class[?] = cls
    while currentClass != null do
      for i <- currentClass.getInterfaces do
        if found.add(i) then collectFrom(i, found)
      currentClass = currentClass.getSuperclass
    found.toList

  private def collectFrom(clazz: Class[?], found: LinkedHashSet[Class[?]]): Unit =
    var currentClass = clazz
    while currentClass != null do
      for i <- currentClass.getInterfaces do
        if found.add(i) then collectFrom(i, found)
      currentClass = currentClass.getSuperclass

  def copyStream(in: InputStream, out: OutputStream, closeStreams: Boolean): Long =
    var count = 0L
    val buf = new Array[Byte](8192)
    try
      var n = in.read(buf)
      while n != -1 do
        out.write(buf, 0, n)
        count += n
        n = in.read(buf)
    finally
      if closeStreams then
        try in.close()
        catch case _: Throwable => ()
        try out.close()
        catch case _: Throwable => ()
    count

/** A cleaner that renders closures serializable if they can be done so safely.
  */
@nowarn("msg=return")
private[sql] object ClosureCleaner extends Logging {
  // Get an ASM class reader for a given class from the JAR that loaded it
  private[common] def getClassReader(cls: Class[_]): ClassReader = {
    // Copy data over, before delegating to ClassReader - else we can run out of open file handles.
    val className = cls.getName.replaceFirst("^.*\\.", "") + ".class"
    val resourceStream = cls.getResourceAsStream(className)
    if (resourceStream == null) {
      null
    } else {
      val baos = new ByteArrayOutputStream(128)

      SparkClassUtilsShim.copyStream(resourceStream, baos, closeStreams = true)
      new ClassReader(new ByteArrayInputStream(baos.toByteArray))
    }
  }

  private[common] def isAmmoniteCommandOrHelper(clazz: Class[_]): Boolean = clazz.getName.matches(
    """^ammonite\.\$sess\.cmd[0-9]*(\$Helper\$?)?"""
  )

  private[common] def isDefinedInAmmonite(clazz: Class[_]): Boolean = clazz.getName.matches(
    """^ammonite\.\$sess\.cmd[0-9]*.*"""
  )

  // Check whether a class represents a Scala closure
  private def isClosure(cls: Class[_]): Boolean =
    cls.getName.contains("$anonfun$")

  // Get a list of the outer objects and their classes of a given closure object, obj;
  // the outer objects are defined as any closures that obj is nested within, plus
  // possibly the class that the outermost closure is in, if any. We stop searching
  // for outer objects beyond that because cloning the user's object is probably
  // not a good idea (whereas we can clone closure objects just fine since we
  // understand how all their fields are used).
  private def getOuterClassesAndObjects(obj: AnyRef): (List[Class[_]], List[AnyRef]) = {
    for (f <- obj.getClass.getDeclaredFields if f.getName == "$outer") {
      f.setAccessible(true)
      val outer = f.get(obj)
      // The outer pointer may be null if we have cleaned this closure before
      if (outer != null) {
        if (isClosure(f.getType)) {
          val recurRet = getOuterClassesAndObjects(outer)
          return (f.getType :: recurRet._1, outer :: recurRet._2)
        } else {
          return (f.getType :: Nil, outer :: Nil) // Stop at the first $outer that is not a closure
        }
      }
    }
    (Nil, Nil)
  }

  /** Return a list of classes that represent closures enclosed in the given closure object.
    */
  private def getInnerClosureClasses(obj: AnyRef): List[Class[_]] = {
    val seen = Set[Class[_]](obj.getClass)
    val stack = Stack[Class[_]](obj.getClass)
    while (!stack.isEmpty) {
      val cr = getClassReader(stack.pop())
      if (cr != null) {
        val set = Set.empty[Class[_]]
        cr.accept(new InnerClosureFinder(set), 0)
        for (cls <- set.diff(seen)) {
          seen += cls
          stack.push(cls)
        }
      }
    }
    seen.diff(Set(obj.getClass)).toList
  }

  /** Initializes the accessed fields for outer classes and their super classes. */
  private def initAccessedFields(
      accessedFields: Map[Class[_], Set[String]],
      outerClasses: Seq[Class[_]]
  ): Unit =
    for (cls <- outerClasses) {
      var currentClass = cls
      assert(currentClass != null, "The outer class can't be null.")

      while (currentClass != null) {
        accessedFields(currentClass) = Set.empty[String]
        currentClass = currentClass.getSuperclass()
      }
    }

  /** Sets accessed fields for given class in clone object based on given object. */
  private def setAccessedFields(
      outerClass: Class[_],
      clone: AnyRef,
      obj: AnyRef,
      accessedFields: Map[Class[_], Set[String]]
  ): Unit =
    for (fieldName <- accessedFields(outerClass)) {
      val field = outerClass.getDeclaredField(fieldName)
      field.setAccessible(true)
      val value = field.get(obj)
      field.set(clone, value)
    }

  /** Clones a given object and sets accessed fields in cloned object. */
  private def cloneAndSetFields(
      parent: AnyRef,
      obj: AnyRef,
      outerClass: Class[_],
      accessedFields: Map[Class[_], Set[String]]
  ): AnyRef = {
    val clone = instantiateClass(outerClass, parent)

    var currentClass = outerClass
    assert(currentClass != null, "The outer class can't be null.")

    while (currentClass != null) {
      setAccessedFields(currentClass, clone, obj, accessedFields)
      currentClass = currentClass.getSuperclass()
    }

    clone
  }

  /** Helper method to clean the given closure in place.
    *
    * The mechanism is to traverse the hierarchy of enclosing closures and null out any references
    * along the way that are not actually used by the starting closure, but are nevertheless
    * included in the compiled anonymous classes. Note that it is unsafe to simply mutate the
    * enclosing closures in place, as other code paths may depend on them. Instead, we clone each
    * enclosing closure and set the parent pointers accordingly.
    *
    * By default, closures are cleaned transitively. This means we detect whether enclosing objects
    * are actually referenced by the starting one, either directly or transitively, and, if not,
    * sever these closures from the hierarchy. In other words, in addition to nulling out unused
    * field references, we also null out any parent pointers that refer to enclosing objects not
    * actually needed by the starting closure. We determine transitivity by tracing through the tree
    * of all methods ultimately invoked by the inner closure and record all the fields referenced in
    * the process.
    *
    * @param func
    *   the starting closure to clean
    * @param cleanTransitively
    *   whether to clean enclosing closures transitively
    * @param accessedFields
    *   a map from a class to a set of its fields that are accessed by the starting closure
    * @return
    *   Some(cleaned closure) if the closure was cleaned, None otherwise.
    */
  private[sql] def clean[F <: AnyRef](
      func: F,
      cleanTransitively: Boolean,
      accessedFields: Map[Class[_], Set[String]]
  ): Option[F] = {
    var cleanedFunc: F = func

    // indylambda check. Most likely to be the case with 2.12, 2.13
    // so we check first
    // non LMF-closures should be less frequent from now on
    val maybeIndylambdaProxy = IndylambdaScalaClosures.getSerializationProxy(func)

    if (!isClosure(func.getClass) && maybeIndylambdaProxy.isEmpty) {
      logDebug(s"Expected a closure; got ${func.getClass.getName}")
      return None
    }

    if (func == null) {
      return None
    }

    if (maybeIndylambdaProxy.isEmpty) {
      cleanNonIndyLambdaClosure(func, cleanTransitively, accessedFields)
    } else {
      val lambdaProxy = maybeIndylambdaProxy.get
      val implMethodName = lambdaProxy.getImplMethodName

      logDebug(s"Cleaning indylambda closure: $implMethodName")

      // capturing class is the class that declared this lambda
      val capturingClassName = lambdaProxy.getCapturingClass.replace('/', '.')
      val classLoader = func.getClass.getClassLoader // this is the safest option
      val capturingClass = Class.forName(capturingClassName, false, classLoader)

      // Fail fast if we detect return statements in closures
      val capturingClassReader = getClassReader(capturingClass)
      if capturingClassReader != null then
        capturingClassReader.accept(new ReturnStatementFinder(Option(implMethodName)), 0)

      val outerThis = if (lambdaProxy.getCapturedArgCount > 0) {
        // only need to clean when there is an enclosing non-null "this" captured by the closure
        Option(lambdaProxy.getCapturedArg(0)).getOrElse(return None)
      } else {
        return None
      }

      // clean only if enclosing "this" is something cleanable, i.e. a Scala REPL line object or
      // Ammonite command helper object.
      if (isDefinedInAmmonite(outerThis.getClass)) {
        IndylambdaScalaClosures.getSerializationProxy(outerThis).foreach { _ =>
          val cleanedOuterThis = clean(outerThis, cleanTransitively, accessedFields)
          if (cleanedOuterThis.isEmpty) {
            return None
          } else {
            return Some(
              cloneIndyLambda(func, cleanedOuterThis.get, lambdaProxy).getOrElse(func)
            )
          }
        }
        cleanedFunc = cleanupAmmoniteReplClosure(func, lambdaProxy, outerThis, cleanTransitively)
      } else {
        val isClosureDeclaredInScalaRepl = capturingClassName.startsWith("$line") &&
          capturingClassName.endsWith("$iw")
        if (isClosureDeclaredInScalaRepl && outerThis.getClass.getName == capturingClassName) {
          assert(accessedFields.isEmpty)
          cleanedFunc = cleanupScalaReplClosure(func, lambdaProxy, outerThis, cleanTransitively)
        }
      }

      logDebug(s" +++ indylambda closure ($implMethodName) is now cleaned +++")
    }

    Some(cleanedFunc)
  }

  /** Cleans non-indylambda closure in place
    */
  private def cleanNonIndyLambdaClosure(
      func: AnyRef,
      cleanTransitively: Boolean,
      accessedFields: Map[Class[_], Set[String]]
  ): Unit = {
    logDebug(s"+++ Cleaning closure $func (${func.getClass.getName}) +++")

    val innerClasses = getInnerClosureClasses(func)
    val (outerClasses, outerObjects) = getOuterClassesAndObjects(func)

    val declaredFields = func.getClass.getDeclaredFields
    val declaredMethods = func.getClass.getDeclaredMethods

    if (log.isDebugEnabled) {
      logDebug(s" + declared fields: ${declaredFields.size}")
      declaredFields.foreach(f => logDebug(s"     $f"))
      logDebug(s" + declared methods: ${declaredMethods.size}")
      declaredMethods.foreach(m => logDebug(s"     $m"))
      logDebug(s" + inner classes: ${innerClasses.size}")
      innerClasses.foreach(c => logDebug(s"     ${c.getName}"))
      logDebug(s" + outer classes: ${outerClasses.size}")
      outerClasses.foreach(c => logDebug(s"     ${c.getName}"))
    }

    // Fail fast if we detect return statements in closures
    val funcReader = getClassReader(func.getClass)
    if funcReader != null then funcReader.accept(new ReturnStatementFinder(), 0)

    // If accessed fields is not populated yet, we assume that
    // the closure we are trying to clean is the starting one
    if (accessedFields.isEmpty) {
      logDebug(" + populating accessed fields because this is the starting closure")
      initAccessedFields(accessedFields, outerClasses)

      for (cls <- func.getClass :: innerClasses) {
        val r = getClassReader(cls)
        if r != null then r.accept(new FieldAccessFinder(accessedFields, cleanTransitively), 0)
      }
    }

    logDebug(s" + fields accessed by starting closure: ${accessedFields.size} classes")
    accessedFields.foreach(f => logDebug("     " + f))

    var outerPairs: List[(Class[_], AnyRef)] = outerClasses.zip(outerObjects).reverse
    var parent: AnyRef = null
    if (outerPairs.nonEmpty) {
      val outermostClass = outerPairs.head._1
      val outermostObject = outerPairs.head._2

      if (isClosure(outermostClass)) {
        logDebug(s" + outermost object is a closure, so we clone it: ${outermostClass}")
      } else if (outermostClass.getName.startsWith("$line")) {
        logDebug(s" + outermost object is a REPL line object, so we clone it:" +
          s" ${outermostClass}")
      } else {
        logDebug(s" + outermost object is not a closure or REPL line object," +
          s" so do not clone it: ${outermostClass}")
        parent = outermostObject
        outerPairs = outerPairs.tail
      }
    } else {
      logDebug(" + there are no enclosing objects!")
    }

    for ((cls, obj) <- outerPairs) {
      logDebug(s" + cloning instance of class ${cls.getName}")
      val clone = cloneAndSetFields(parent, obj, cls, accessedFields)

      if (cleanTransitively && isClosure(clone.getClass)) {
        logDebug(s" + cleaning cloned closure recursively (${cls.getName})")
        clean(clone, cleanTransitively, accessedFields)
      }
      parent = clone
    }

    if (parent != null) {
      val field = func.getClass.getDeclaredField("$outer")
      field.setAccessible(true)
      if (
        accessedFields.contains(func.getClass) &&
        !accessedFields(func.getClass).contains("$outer")
      ) {
        logDebug(s" + the starting closure doesn't actually need $parent, so we null it out")
        field.set(func, null)
      } else {
        field.set(func, parent)
      }
    }

    logDebug(s" +++ closure $func (${func.getClass.getName}) is now cleaned +++")
  }

  private def cleanupScalaReplClosure[F <: AnyRef](
      func: F,
      lambdaProxy: SerializedLambda,
      outerThis: AnyRef,
      cleanTransitively: Boolean
  ): F = {

    val capturingClass = outerThis.getClass
    val accessedFields: Map[Class[_], Set[String]] = Map.empty
    initAccessedFields(accessedFields, Seq(capturingClass))

    IndylambdaScalaClosures.findAccessedFields(
      lambdaProxy,
      func.getClass.getClassLoader,
      accessedFields,
      Map.empty,
      Map.empty,
      cleanTransitively
    )

    logDebug(s" + fields accessed by starting closure: ${accessedFields.size} classes")
    accessedFields.foreach(f => logDebug("     " + f))

    if (accessedFields(capturingClass).size < capturingClass.getDeclaredFields.length) {
      logDebug(s" + cloning instance of REPL class ${capturingClass.getName}")
      val clonedOuterThis = cloneAndSetFields(
        parent = null,
        outerThis,
        capturingClass,
        accessedFields
      )
      cloneIndyLambda(func, clonedOuterThis, lambdaProxy).getOrElse {
        val outerField = func.getClass.getDeclaredField("arg$1")
        setFieldAndIgnoreModifiers(func, outerField, clonedOuterThis)
        func
      }
    } else {
      func
    }
  }

  private def cleanupAmmoniteReplClosure[F <: AnyRef](
      func: F,
      lambdaProxy: SerializedLambda,
      outerThis: AnyRef,
      cleanTransitively: Boolean
  ): F = {

    val accessedFields: Map[Class[_], Set[String]] = Map.empty
    initAccessedFields(accessedFields, Seq(outerThis.getClass))

    val ammCmdInstances: Map[Class[_], AnyRef] = Map.empty
    val accessedAmmCmdFields: Map[Class[_], Set[String]] = Map.empty
    if (isAmmoniteCommandOrHelper(outerThis.getClass)) {
      ammCmdInstances(outerThis.getClass) = outerThis
      accessedAmmCmdFields(outerThis.getClass) = Set.empty
    }

    IndylambdaScalaClosures.findAccessedFields(
      lambdaProxy,
      func.getClass.getClassLoader,
      accessedFields,
      accessedAmmCmdFields,
      ammCmdInstances,
      cleanTransitively
    )

    logTrace(s" + command fields accessed by starting closure: " +
      s"${accessedAmmCmdFields.size} classes")
    accessedAmmCmdFields.foreach(f => logTrace("     " + f))

    val cmdClones = Map[Class[_], AnyRef]()
    for ((cmdClass, _) <- ammCmdInstances if !cmdClass.getName.contains("Helper")) {
      logDebug(s" + Cloning instance of Ammonite command class ${cmdClass.getName}")
      cmdClones(cmdClass) = instantiateClass(cmdClass, enclosingObject = null)
    }
    for (
      (cmdHelperClass, cmdHelperInstance) <- ammCmdInstances
      if cmdHelperClass.getName.contains("Helper")
    ) {
      val cmdHelperOuter = cmdHelperClass.getDeclaredFields
        .find(_.getName == "$outer")
        .map { field =>
          field.setAccessible(true)
          field.get(cmdHelperInstance)
        }
      val outerClone = cmdHelperOuter.flatMap(o => cmdClones.get(o.getClass)).orNull
      logDebug(s" + Cloning instance of Ammonite command helper class ${cmdHelperClass.getName}")
      cmdClones(cmdHelperClass) =
        instantiateClass(cmdHelperClass, enclosingObject = outerClone)
    }

    for ((_, cmdClone) <- cmdClones) {
      val cmdClass = cmdClone.getClass
      val accessedAmmFields = accessedAmmCmdFields(cmdClass)
      for (
        field <- cmdClone.getClass.getDeclaredFields
        if accessedAmmFields.contains(field.getName) && field.getName != "$outer"
      ) {
        val value = cmdClones.getOrElse(
          field.getType, {
            field.setAccessible(true)
            field.get(ammCmdInstances(cmdClass))
          }
        )
        setFieldAndIgnoreModifiers(cmdClone, field, value)
      }
    }

    val outerThisClone = if (!isAmmoniteCommandOrHelper(outerThis.getClass)) {
      logDebug(s" + Cloning instance of lambda capturing class ${outerThis.getClass.getName}")
      val clone = cloneAndSetFields(parent = null, outerThis, outerThis.getClass, accessedFields)
      for (field <- outerThis.getClass.getDeclaredFields) {
        field.setAccessible(true)
        cmdClones.get(field.getType).foreach { value =>
          setFieldAndIgnoreModifiers(clone, field, value)
        }
      }
      clone
    } else {
      cmdClones(outerThis.getClass)
    }

    cloneIndyLambda(func, outerThisClone, lambdaProxy).getOrElse {
      val outerField = func.getClass.getDeclaredField("arg$1")
      setFieldAndIgnoreModifiers(func, outerField, outerThisClone)
      func
    }
  }

  private def setFieldAndIgnoreModifiers(obj: AnyRef, field: Field, value: AnyRef): Unit = {
    val modifiersField = getFinalModifiersFieldForJava17(field)
    modifiersField
      .foreach(m => m.setInt(field, field.getModifiers & ~Modifier.FINAL))
    field.setAccessible(true)
    field.set(obj, value)

    modifiersField
      .foreach(m => m.setInt(field, field.getModifiers | Modifier.FINAL))
  }

  /** This method is used to get the final modifier field when on Java 17.
    */
  private def getFinalModifiersFieldForJava17(field: Field): Option[Field] =
    if (Modifier.isFinal(field.getModifiers)) {
      try {
        val methodGetDeclaredFields0 = classOf[Class[_]]
          .getDeclaredMethod("getDeclaredFields0", classOf[Boolean])
        methodGetDeclaredFields0.setAccessible(true)
        val fields = methodGetDeclaredFields0.invoke(classOf[Field], false.asInstanceOf[Object])
          .asInstanceOf[Array[Field]]
        val modifiersFieldOption = fields.find(field => "modifiers".equals(field.getName))
        modifiersFieldOption.foreach(_.setAccessible(true))
        modifiersFieldOption
      } catch {
        case _: Throwable => None
      }
    } else None

  private def instantiateClass(cls: Class[_], enclosingObject: AnyRef): AnyRef = {
    // Use reflection to instantiate object without calling constructor
    val rf = sun.reflect.ReflectionFactory.getReflectionFactory()
    val parentCtor = classOf[java.lang.Object].getDeclaredConstructor()
    val newCtor = rf.newConstructorForSerialization(cls, parentCtor)
    val obj = newCtor.newInstance().asInstanceOf[AnyRef]
    if (enclosingObject != null) {
      val field = cls.getDeclaredField("$outer")
      field.setAccessible(true)
      field.set(obj, enclosingObject)
    }
    obj
  }

  private def cloneIndyLambda[F <: AnyRef](
      indyLambda: F,
      outerThis: AnyRef,
      lambdaProxy: SerializedLambda
  ): Option[F] = {
    val javaVersion = Runtime.version().feature()
    val useClone = System.getProperty("spark.cloneBasedClosureCleaner.enabled") == "true" ||
      System.getenv("SPARK_CLONE_BASED_CLOSURE_CLEANER") == "1" || javaVersion >= 22 ||
      (javaVersion >= 18 &&
        System.getProperty("jdk.reflect.useDirectMethodHandle", "true") == "true")

    if (useClone) {
      try {
        val factory = makeClonedIndyLambdaFactory(indyLambda.getClass, lambdaProxy)

        val argsBuffer = new ArrayBuffer[Object]()
        var i = 0
        while (i < lambdaProxy.getCapturedArgCount) {
          val arg = lambdaProxy.getCapturedArg(i)
          argsBuffer.append(arg)
          i += 1
        }
        val clonedLambda =
          factory.invokeWithArguments(outerThis +: argsBuffer.tail.toArray: _*).asInstanceOf[F]
        Some(clonedLambda)
      } catch {
        case _: Throwable => None
      }
    } else {
      None
    }
  }

  private def makeClonedIndyLambdaFactory(
      originalFuncClass: Class[_],
      lambdaProxy: SerializedLambda
  ): MethodHandle = {
    val classLoader = originalFuncClass.getClassLoader

    val fInterface = Class.forName(
      lambdaProxy.getFunctionalInterfaceClass.replace("/", "."),
      false,
      classLoader
    )
    val numCapturedArgs = lambdaProxy.getCapturedArgCount
    val implMethodType = MethodType.fromMethodDescriptorString(
      lambdaProxy.getImplMethodSignature,
      classLoader
    )
    val invokedMethodType = MethodType.methodType(
      fInterface,
      (0 until numCapturedArgs).map(i => implMethodType.parameterType(i)).toArray
    )

    val implClassName = lambdaProxy.getImplClass.replace("/", ".")
    val implClass = Class.forName(implClassName, false, classLoader)
    val replLookup = getFullPowerLookupFor(implClass)

    val implMethodName = lambdaProxy.getImplMethodName
    val implMethodHandle = replLookup.findStatic(implClass, implMethodName, implMethodType)
    val funcMethodType = MethodType.fromMethodDescriptorString(
      lambdaProxy.getFunctionalInterfaceMethodSignature,
      classLoader
    )
    val instantiatedMethodType = MethodType.fromMethodDescriptorString(
      lambdaProxy.getInstantiatedMethodType,
      classLoader
    )

    val callSite = LambdaMetafactory.altMetafactory(
      replLookup,
      lambdaProxy.getFunctionalInterfaceMethodName,
      invokedMethodType,
      funcMethodType,
      implMethodHandle,
      instantiatedMethodType,
      LambdaMetafactory.FLAG_SERIALIZABLE
    )

    callSite.getTarget
  }

  private def getFullPowerLookupFor(targetClass: Class[_]): MethodHandles.Lookup = {
    val replLookupCtor = classOf[MethodHandles.Lookup].getDeclaredConstructor(
      classOf[Class[_]],
      classOf[Class[_]],
      classOf[Int]
    )
    replLookupCtor.setAccessible(true)
    // -1 means full-power.
    replLookupCtor.newInstance(targetClass, null, -1)
  }
}

@nowarn("msg=(return|unused explicit parameter)")
private[common] object IndylambdaScalaClosures extends Logging {
  // internal name of java.lang.invoke.LambdaMetafactory
  val LambdaMetafactoryClassName = "java/lang/invoke/LambdaMetafactory"
  // the method that Scala indylambda use for bootstrap method
  val LambdaMetafactoryMethodName = "altMetafactory"
  val LambdaMetafactoryMethodDesc = "(Ljava/lang/invoke/MethodHandles$Lookup;" +
    "Ljava/lang/String;Ljava/lang/invoke/MethodType;[Ljava/lang/Object;)" +
    "Ljava/lang/invoke/CallSite;"

  /** Check if the given reference is a indylambda style Scala closure.
    */
  def getSerializationProxy(maybeClosure: AnyRef): Option[SerializedLambda] = {
    def isClosureCandidate(cls: Class[_]): Boolean = {
      val implementedInterfaces = SparkClassUtilsShim.getAllInterfaces(cls)
      implementedInterfaces.exists(_.getName.startsWith("scala.Function"))
    }

    maybeClosure.getClass match {
      case c if !c.isSynthetic || !maybeClosure.isInstanceOf[Serializable] => None

      case c if isClosureCandidate(c) =>
        try
          Option(inspect(maybeClosure)).filter(isIndylambdaScalaClosure)
        catch {
          case e: Exception =>
            logDebug("The given reference is not an indylambda Scala closure.", e)
            None
        }

      case _ => None
    }
  }

  def isIndylambdaScalaClosure(lambdaProxy: SerializedLambda): Boolean =
    lambdaProxy.getImplMethodKind == MethodHandleInfo.REF_invokeStatic &&
      lambdaProxy.getImplMethodName.contains("$anonfun$")

  def inspect(closure: AnyRef): SerializedLambda = {
    val writeReplace = closure.getClass.getDeclaredMethod("writeReplace")
    writeReplace.setAccessible(true)
    writeReplace.invoke(closure).asInstanceOf[SerializedLambda]
  }

  def isLambdaMetafactory(bsmHandle: Handle): Boolean =
    bsmHandle.getOwner == LambdaMetafactoryClassName &&
      bsmHandle.getName == LambdaMetafactoryMethodName &&
      bsmHandle.getDesc == LambdaMetafactoryMethodDesc

  def isLambdaBodyCapturingOuter(handle: Handle, ownerInternalName: String): Boolean =
    handle.getTag == Opcodes.H_INVOKESTATIC &&
      handle.getName.contains("$anonfun$") &&
      handle.getOwner == ownerInternalName &&
      handle.getDesc.startsWith(s"(L$ownerInternalName;")

  def isInnerClassCtorCapturingOuter(
      op: Int,
      owner: String,
      name: String,
      desc: String,
      callerInternalName: String
  ): Boolean =
    op == Opcodes.INVOKESPECIAL && name == "<init>" && desc.startsWith(s"(L$callerInternalName;")

  def findAccessedFields(
      lambdaProxy: SerializedLambda,
      lambdaClassLoader: ClassLoader,
      accessedFields: Map[Class[_], Set[String]],
      accessedAmmCmdFields: Map[Class[_], Set[String]],
      ammCmdInstances: Map[Class[_], AnyRef],
      findTransitively: Boolean
  ): Unit = {

    val classInfoByInternalName = Map.empty[String, (Class[_], ClassNode)]
    val methodNodeById = Map.empty[MethodIdentifier[_], MethodNode]
    def getOrUpdateClassInfo(classInternalName: String): (Class[_], ClassNode) = {
      val classInfo = classInfoByInternalName.getOrElseUpdate(
        classInternalName, {
          val classExternalName = classInternalName.replace('/', '.')
          val clazz = Class.forName(classExternalName, false, lambdaClassLoader)

          def getClassNode(clazz: Class[_]): ClassNode = {
            val classNode = new ClassNode()
            val classReader = ClosureCleaner.getClassReader(clazz)
            if classReader != null then classReader.accept(classNode, 0)
            classNode
          }

          var curClazz = clazz
          while (curClazz != null) {
            for (m <- getClassNode(curClazz).methods.asScala)
              methodNodeById(MethodIdentifier(clazz, m.name, m.desc)) = m
            curClazz = curClazz.getSuperclass
          }

          (clazz, getClassNode(clazz))
        }
      )
      classInfo
    }

    val implClassInternalName = lambdaProxy.getImplClass
    val (implClass, _) = getOrUpdateClassInfo(implClassInternalName)

    val implMethodId = MethodIdentifier(
      implClass,
      lambdaProxy.getImplMethodName,
      lambdaProxy.getImplMethodSignature
    )

    val trackedClassInternalNames = Set[String](implClassInternalName)

    val visited = Set.empty[MethodIdentifier[_]]
    val queue = Queue[MethodIdentifier[_]](implMethodId)
    def pushIfNotVisited(methodId: MethodIdentifier[_]): Unit =
      if (!visited.contains(methodId)) {
        queue.enqueue(methodId)
      }

    def addAmmoniteCommandFieldsToTracking(currentClass: Class[_]): Unit = {
      val currentInstance = if (currentClass == lambdaProxy.getCapturedArg(0).getClass) {
        Some(lambdaProxy.getCapturedArg(0))
      } else {
        ammCmdInstances.get(currentClass)
      }
      currentInstance.foreach { cmdInstance =>
        for (
          otherCmdField <- cmdInstance.getClass.getDeclaredFields
          if ClosureCleaner.isAmmoniteCommandOrHelper(otherCmdField.getType)
        ) {
          otherCmdField.setAccessible(true)
          val otherCmdHelperRef = otherCmdField.get(cmdInstance)
          val otherCmdClass = otherCmdField.getType
          if (otherCmdHelperRef != null && !ammCmdInstances.contains(otherCmdClass)) {
            logTrace(s"      started tracking ${otherCmdClass.getName} Ammonite object")
            ammCmdInstances(otherCmdClass) = otherCmdHelperRef
            accessedAmmCmdFields(otherCmdClass) = Set()
          }
        }
      }
    }

    while (queue.nonEmpty) {
      val currentId = queue.dequeue()
      visited += currentId

      val currentClass = currentId.cls
      addAmmoniteCommandFieldsToTracking(currentClass)
      val currentMethodNode = methodNodeById(currentId)
      logTrace(s"  scanning ${currentId.cls.getName}.${currentId.name}${currentId.desc}")
      if currentMethodNode != null then
        currentMethodNode.accept(new MethodVisitor(Opcodes.ASM9) {
          val currentClassName = currentClass.getName
          val currentClassInternalName = currentClassName.replace('.', '/')

          override def visitFieldInsn(op: Int, owner: String, name: String, desc: String): Unit =
            if (op == Opcodes.GETFIELD || op == Opcodes.PUTFIELD) {
              val ownerExternalName = owner.replace('/', '.')
              for (cl <- accessedFields.keys if cl.getName == ownerExternalName) {
                logTrace(s"    found field access $name on $ownerExternalName")
                accessedFields(cl) += name
              }
              for (cl <- accessedAmmCmdFields.keys if cl.getName == ownerExternalName) {
                logTrace(s"    found Ammonite command field access $name on $ownerExternalName")
                accessedAmmCmdFields(cl) += name
              }
            }

          override def visitMethodInsn(
              op: Int,
              owner: String,
              name: String,
              desc: String,
              itf: Boolean
          ): Unit = {
            val ownerExternalName = owner.replace('/', '.')
            if (owner == currentClassInternalName) {
              logTrace(s"    found intra class call to $ownerExternalName.$name$desc")
              pushIfNotVisited(MethodIdentifier(currentClass, name, desc))
            } else if (owner.startsWith("ammonite/$sess/cmd")) {
              val classInfo = getOrUpdateClassInfo(owner)
              pushIfNotVisited(MethodIdentifier(classInfo._1, name, desc))
            } else if (
              isInnerClassCtorCapturingOuter(
                op,
                owner,
                name,
                desc,
                currentClassInternalName
              )
            ) {
              logDebug(s"    found inner class $ownerExternalName")
              val innerClassInfo = getOrUpdateClassInfo(owner)
              val innerClass = innerClassInfo._1
              val innerClassNode = innerClassInfo._2
              trackedClassInternalNames += owner
              for (m <- innerClassNode.methods.asScala) {
                logTrace(s"      found method ${m.name}${m.desc}")
                pushIfNotVisited(MethodIdentifier(innerClass, m.name, m.desc))
              }
            } else if (findTransitively && trackedClassInternalNames.contains(owner)) {
              logTrace(s"    found call to outer $ownerExternalName.$name$desc")
              val (calleeClass, _) = getOrUpdateClassInfo(owner)
              pushIfNotVisited(MethodIdentifier(calleeClass, name, desc))
            } else {
              logTrace(s"    ignoring call to $ownerExternalName.$name$desc")
            }
          }

          override def visitInvokeDynamicInsn(
              name: String,
              desc: String,
              bsmHandle: Handle,
              bsmArgs: Object*
          ): Unit = {
            logTrace(s"    invokedynamic: $name$desc, bsmHandle=$bsmHandle, bsmArgs=$bsmArgs")

            if (!name.startsWith("apply")) return
            if (!Type.getReturnType(desc).getDescriptor.startsWith("Lscala/Function")) return

            if (isLambdaMetafactory(bsmHandle)) {
              val targetHandle = bsmArgs(1).asInstanceOf[Handle]
              if (isLambdaBodyCapturingOuter(targetHandle, currentClassInternalName)) {
                logDebug(s"    found inner closure $targetHandle")
                val calleeMethodId =
                  MethodIdentifier(currentClass, targetHandle.getName, targetHandle.getDesc)
                pushIfNotVisited(calleeMethodId)
              }
            }
          }
        })
    }
  }
}

private[common] class ReturnStatementInClosureException
    extends SparkException("Return statements aren't allowed in Spark closures")

private class ReturnStatementFinder(targetMethodName: Option[String] = None)
    extends ClassVisitor(Opcodes.ASM9) {
  override def visitMethod(
      access: Int,
      name: String,
      desc: String,
      sig: String,
      exceptions: Array[String]
  ): MethodVisitor =

    if (name.contains("apply") || name.contains("$anonfun$")) {
      val isTargetMethod = targetMethodName.isEmpty ||
        name == targetMethodName.get || name == targetMethodName.get.stripSuffix("$adapted")

      new MethodVisitor(Opcodes.ASM9) {
        override def visitTypeInsn(op: Int, tp: String): Unit =
          if (
            op == Opcodes.NEW && tp.contains("scala/runtime/NonLocalReturnControl") &&
            isTargetMethod
          ) {
            throw new ReturnStatementInClosureException
          }
      }
    } else {
      new MethodVisitor(Opcodes.ASM9) {}
    }
}

/** Helper class to identify a method. */
private case class MethodIdentifier[T](cls: Class[T], name: String, desc: String)

private[common] class FieldAccessFinder(
    fields: Map[Class[_], Set[String]],
    findTransitively: Boolean,
    specificMethod: Option[MethodIdentifier[_]] = None,
    visitedMethods: Set[MethodIdentifier[_]] = Set.empty
) extends ClassVisitor(Opcodes.ASM9) {

  override def visitMethod(
      access: Int,
      name: String,
      desc: String,
      sig: String,
      exceptions: Array[String]
  ): MethodVisitor = {

    if (
      specificMethod.isDefined &&
      (specificMethod.get.name != name || specificMethod.get.desc != desc)
    ) {
      return null
    }

    new MethodVisitor(Opcodes.ASM9) {
      override def visitFieldInsn(op: Int, owner: String, name: String, desc: String): Unit =
        if (op == Opcodes.GETFIELD) {
          for (cl <- fields.keys if cl.getName == owner.replace('/', '.'))
            fields(cl) += name
        }

      override def visitMethodInsn(
          op: Int,
          owner: String,
          name: String,
          desc: String,
          itf: Boolean
      ): Unit =
        for (cl <- fields.keys if cl.getName == owner.replace('/', '.')) {
          if (op == Opcodes.INVOKEVIRTUAL && owner.endsWith("$iwC") && !name.endsWith("$outer")) {
            fields(cl) += name
          }
          if (findTransitively) {
            val m = MethodIdentifier(cl, name, desc)
            if (!visitedMethods.contains(m)) {
              visitedMethods += m

              var currentClass = cl
              assert(currentClass != null, "The outer class can't be null.")

              while (currentClass != null) {
                val r = ClosureCleaner.getClassReader(currentClass)
                if r != null then
                  r.accept(
                    new FieldAccessFinder(fields, findTransitively, Some(m), visitedMethods),
                    0
                  )
                currentClass = currentClass.getSuperclass()
              }
            }
          }
        }
    }
  }
}

private class InnerClosureFinder(output: Set[Class[_]]) extends ClassVisitor(Opcodes.ASM9) {
  var myName: String = null

  override def visit(
      version: Int,
      access: Int,
      name: String,
      sig: String,
      superName: String,
      interfaces: Array[String]
  ): Unit =
    myName = name

  override def visitMethod(
      access: Int,
      name: String,
      desc: String,
      sig: String,
      exceptions: Array[String]
  ): MethodVisitor =
    new MethodVisitor(Opcodes.ASM9) {
      override def visitMethodInsn(
          op: Int,
          owner: String,
          name: String,
          desc: String,
          itf: Boolean
      ): Unit = {
        val argTypes = Type.getArgumentTypes(desc)
        if (
          op == Opcodes.INVOKESPECIAL && name == "<init>" && argTypes.length > 0
          && argTypes(0).toString.startsWith("L")
          && argTypes(0).getInternalName == myName
        ) {
          output += SparkClassUtilsShim.classForName(
            owner.replace('/', '.'),
            initialize = false,
            noSparkClassLoader = true
          )
        }
      }
    }
}
