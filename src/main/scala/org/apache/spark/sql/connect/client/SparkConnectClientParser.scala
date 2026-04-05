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
package org.apache.spark.sql.connect.client

import scala.annotation.tailrec

/** Parser that takes an array of (CLI) arguments and produces a [[Config]].
  *
  * Ported from the upstream `SparkConnectClientParser` but adapted for SC3's
  * `SparkConnectClient.create(url)` factory method (no Builder pattern).
  */
private[sql] object SparkConnectClientParser:

  /** Parsed CLI configuration. */
  case class Config(
      remote: Option[String] = None,
      host: String = "localhost",
      port: Int = 15002,
      useSsl: Boolean = false,
      token: Option[String] = None,
      userId: Option[String] = None,
      userName: Option[String] = None,
      sessionId: Option[String] = None,
      options: Map[String, String] = Map.empty
  )

  def usage(): String =
    s"""
       |Options:
       |   --remote REMOTE              URI of the Spark Connect Server to connect to.
       |   --host HOST                  Host where the Spark Connect Server is running.
       |   --port PORT                  Port where the Spark Connect Server is running.
       |   --use_ssl                    Connect to the server using SSL.
       |   --token TOKEN                Token to use for authentication.
       |   --user_id USER_ID            Id of the user connecting.
       |   --user_name USER_NAME        Name of the user connecting.
       |   --session_id SESSION_ID      Session Id of the user connecting.
       |   --option KEY=VALUE           Key-value pair that is used to further configure the session.
     """.stripMargin

  /** Parse CLI arguments into a [[Config]]. */
  def parse(args: Array[String]): Config =
    parseImpl(args.toList, Config())

  @tailrec
  private def parseImpl(args: List[String], config: Config): Config =
    args match
      case Nil                => config
      case "--remote" :: tail =>
        val (value, remainder) = extract("--remote", tail)
        parseImpl(remainder, config.copy(remote = Some(value)))
      case "--host" :: tail =>
        val (value, remainder) = extract("--host", tail)
        parseImpl(remainder, config.copy(host = value))
      case "--port" :: tail =>
        val (value, remainder) = extract("--port", tail)
        parseImpl(remainder, config.copy(port = value.toInt))
      case "--token" :: tail =>
        val (value, remainder) = extract("--token", tail)
        parseImpl(remainder, config.copy(token = Some(value)))
      case "--use_ssl" :: tail =>
        parseImpl(tail, config.copy(useSsl = true))
      case "--user_id" :: tail =>
        val (value, remainder) = extract("--user_id", tail)
        parseImpl(remainder, config.copy(userId = Some(value)))
      case "--user_name" :: tail =>
        val (value, remainder) = extract("--user_name", tail)
        parseImpl(remainder, config.copy(userName = Some(value)))
      case "--session_id" :: tail =>
        val (value, remainder) = extract("--session_id", tail)
        parseImpl(remainder, config.copy(sessionId = Some(value)))
      case "--option" :: tail =>
        if tail.isEmpty then throw IllegalArgumentException("--option requires a key-value pair")
        val kv = tail.head.split("=", 2)
        if kv.length != 2 then
          throw IllegalArgumentException(
            s"--option should contain key=value, found ${tail.head} instead"
          )
        parseImpl(tail.tail, config.copy(options = config.options + (kv(0) -> kv(1))))
      case unsupported :: _ =>
        throw IllegalArgumentException(s"$unsupported is an unsupported argument.")

  /** Build a `sc://` URL from a [[Config]]. */
  def buildUrl(config: Config): String =
    val base = s"sc://${config.host}:${config.port}"
    val params = Seq.newBuilder[String]
    if config.useSsl then params += "use_ssl=true"
    config.token.foreach(t => params += s"token=$t")
    config.userId.foreach(u => params += s"user_id=$u")
    config.userName.foreach(u => params += s"user_name=$u")
    config.sessionId.foreach(s => params += s"session_id=$s")
    config.options.foreach((k, v) => params += s"$k=$v")
    val paramSeq = params.result()
    if paramSeq.isEmpty then base
    else base + ";" + paramSeq.mkString(";")

  private def extract(name: String, args: List[String]): (String, List[String]) =
    require(args.nonEmpty, s"$name option requires a value")
    (args.head, args.tail)
