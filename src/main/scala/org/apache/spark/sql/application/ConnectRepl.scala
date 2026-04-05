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

import java.io.{InputStream, OutputStream}

import scala.util.control.NonFatal

import ammonite.compiler.CodeClassWrapper

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connect.client.SparkConnectClientParser

/** Ammonite-based REPL for Spark Connect (Scala 3).
  *
  * Usage:
  * {{{
  *   // Connect to default server (localhost:15002)
  *   build/sbt run
  *
  *   // Connect to a specific server
  *   build/sbt "run --remote sc://myhost:15002"
  *
  *   // Or use the SPARK_REMOTE environment variable
  *   SPARK_REMOTE=sc://myhost:15002 build/sbt run
  * }}}
  */
object ConnectRepl:
  private val name = "Spark Connect REPL (Scala 3)"

  private[application] val splash: String = """Welcome to
      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /___/ .__/\_,_/_/ /_/\_\   version %s
      /_/

Spark Connect Scala 3 REPL.
Spark session available as 'spark'.
   """

  private[application] val predefCode: String =
    """
      |import org.apache.spark.sql.functions.*
      |import org.apache.spark.sql.implicits.*
      |import org.apache.spark.sql.connect.client.AmmoniteClassFinder
      |given org.apache.spark.sql.SparkSession = spark
      |spark.registerClassFinder(new AmmoniteClassFinder(repl.sess))
      |""".stripMargin

  def main(args: Array[String]): Unit = doMain(args)

  private[application] def doMain(
      args: Array[String],
      inputStream: InputStream = System.in,
      outputStream: OutputStream = System.out,
      errorStream: OutputStream = System.err
  ): Unit =
    // 1. Parse CLI args
    val config =
      try SparkConnectClientParser.parse(args)
      catch
        case NonFatal(e) =>
          // scalastyle:off println
          println(s"""
             |$name
             |${e.getMessage}
             |${SparkConnectClientParser.usage()}
             |""".stripMargin)
          // scalastyle:on println
          sys.exit(1)

    // 2. Determine the connection URL:
    //    CLI --remote > SPARK_REMOTE env var > build from host/port/params
    val url = config.remote.getOrElse(
      sys.env.getOrElse("SPARK_REMOTE", SparkConnectClientParser.buildUrl(config))
    )

    // 3. Build SparkSession
    val builder = SparkSession.builder().remote(url)
    config.options.foreach((k, v) => builder.config(k, v))
    val spark = builder.build()

    // 4. Run the Ammonite REPL.
    //    We use CodeClassWrapper (generates classes, not objects) for better serialization
    //    behavior when using UDFs.
    val main = ammonite.Main(
      welcomeBanner = Option(splash.format(spark.version)),
      predefCode = predefCode,
      replCodeWrapper = CodeClassWrapper,
      scriptCodeWrapper = CodeClassWrapper,
      inputStream = inputStream,
      outputStream = outputStream,
      errorStream = errorStream
    )

    main.run(ammonite.util.Bind("spark", spark))
