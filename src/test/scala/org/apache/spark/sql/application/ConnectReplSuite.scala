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
package org.apache.spark.sql.application

import org.scalatest.funsuite.AnyFunSuite

class ConnectReplSuite extends AnyFunSuite:

  test("splash contains Spark ASCII art") {
    val splash = ConnectRepl.splash
    assert(splash.contains("____"))
    assert(splash.contains("/ __/__  ___ _____/ /__"))
    assert(splash.contains("version %s"))
  }

  test("splash can be formatted with a version") {
    val formatted = ConnectRepl.splash.format("4.1.0")
    assert(formatted.contains("version 4.1.0"))
    assert(!formatted.contains("%s"))
  }

  test("predef code contains required imports") {
    val predef = ConnectRepl.predefCode
    assert(predef.contains("import org.apache.spark.sql.functions.*"))
    assert(predef.contains("import org.apache.spark.sql.implicits.*"))
    assert(predef.contains("AmmoniteClassFinder"))
    assert(predef.contains("registerClassFinder"))
  }

  test("predef code provides given SparkSession") {
    val predef = ConnectRepl.predefCode
    assert(predef.contains("given org.apache.spark.sql.SparkSession = spark"))
  }
