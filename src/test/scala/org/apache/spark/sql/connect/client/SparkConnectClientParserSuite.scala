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

import org.scalatest.funsuite.AnyFunSuite

class SparkConnectClientParserSuite extends AnyFunSuite:

  import SparkConnectClientParser.*

  test("parse --remote") {
    val config = parse(Array("--remote", "sc://myhost:15003"))
    assert(config.remote.contains("sc://myhost:15003"))
  }

  test("parse --host and --port") {
    val config = parse(Array("--host", "example.com", "--port", "9999"))
    assert(config.host == "example.com")
    assert(config.port == 9999)
  }

  test("parse --token and --use_ssl") {
    val config = parse(Array("--use_ssl", "--token", "my-secret"))
    assert(config.useSsl)
    assert(config.token.contains("my-secret"))
  }

  test("parse --user_id and --user_name") {
    val config = parse(Array("--user_id", "uid123", "--user_name", "alice"))
    assert(config.userId.contains("uid123"))
    assert(config.userName.contains("alice"))
  }

  test("parse --session_id") {
    val config = parse(Array("--session_id", "sess-42"))
    assert(config.sessionId.contains("sess-42"))
  }

  test("parse --option key=value") {
    val config = parse(Array("--option", "spark.sql.shuffle.partitions=10"))
    assert(config.options == Map("spark.sql.shuffle.partitions" -> "10"))
  }

  test("parse multiple --option") {
    val config = parse(Array(
      "--option",
      "k1=v1",
      "--option",
      "k2=v2"
    ))
    assert(config.options == Map("k1" -> "v1", "k2" -> "v2"))
  }

  test("default config uses localhost:15002") {
    val config = parse(Array.empty)
    assert(config.host == "localhost")
    assert(config.port == 15002)
    assert(config.remote.isEmpty)
    assert(!config.useSsl)
    assert(config.token.isEmpty)
    assert(config.options.isEmpty)
  }

  test("unknown argument throws IllegalArgumentException") {
    assertThrows[IllegalArgumentException] {
      parse(Array("--unknown"))
    }
  }

  test("--option without value throws IllegalArgumentException") {
    assertThrows[IllegalArgumentException] {
      parse(Array("--option", "no-equals-sign"))
    }
  }

  test("--port without value throws IllegalArgumentException") {
    assertThrows[IllegalArgumentException] {
      parse(Array("--port"))
    }
  }

  // --- buildUrl tests ---

  test("buildUrl with defaults") {
    val url = buildUrl(Config())
    assert(url == "sc://localhost:15002")
  }

  test("buildUrl with host and port") {
    val url = buildUrl(Config(host = "myhost", port = 9999))
    assert(url == "sc://myhost:9999")
  }

  test("buildUrl with SSL and token") {
    val url = buildUrl(Config(useSsl = true, token = Some("tok")))
    assert(url.startsWith("sc://localhost:15002;"))
    assert(url.contains("use_ssl=true"))
    assert(url.contains("token=tok"))
  }

  test("buildUrl with options") {
    val url = buildUrl(Config(options = Map("k1" -> "v1")))
    assert(url == "sc://localhost:15002;k1=v1")
  }

  test("buildUrl with all parameters") {
    val url = buildUrl(Config(
      host = "h",
      port = 1234,
      useSsl = true,
      token = Some("t"),
      userId = Some("u"),
      userName = Some("n"),
      sessionId = Some("s"),
      options = Map("a" -> "b")
    ))
    assert(url.startsWith("sc://h:1234;"))
    assert(url.contains("use_ssl=true"))
    assert(url.contains("token=t"))
    assert(url.contains("user_id=u"))
    assert(url.contains("user_name=n"))
    assert(url.contains("session_id=s"))
    assert(url.contains("a=b"))
  }

  // --- usage tests ---

  test("usage() contains all option names") {
    val u = usage()
    assert(u.contains("--remote"))
    assert(u.contains("--host"))
    assert(u.contains("--port"))
    assert(u.contains("--use_ssl"))
    assert(u.contains("--token"))
    assert(u.contains("--user_id"))
    assert(u.contains("--user_name"))
    assert(u.contains("--session_id"))
    assert(u.contains("--option"))
  }
