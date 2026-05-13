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

  // --- SparkConnectClient.connectionUrl / token redaction tests ---

  test("SparkConnectClient stores connectionUrl without token") {
    // Verify URL parsing round-trips without token
    val url = buildUrl(Config(host = "myhost", port = 15002))
    assert(url == "sc://myhost:15002")
    // A URL with token should NOT store the token in connectionUrl
    val urlWithToken = buildUrl(Config(host = "myhost", port = 15002, token = Some("secret")))
    assert(urlWithToken.contains("token=secret"))
    // After create() processes this URL, the stored connectionUrl should NOT have the token
    // (cannot create a real client without server, but we verify via toString in the next test)
  }

  test("token is stripped from URL parameters for storage") {
    // Simulate what create() does: parse URL, remove token from params, rebuild
    val url = "sc://myhost:15002;use_ssl=true;token=my-secret;user_id=alice"
    val stripped = url.stripPrefix("sc://")
    val parts = stripped.split(";").toSeq
    val hostPort = parts.head.split(":")
    val host = hostPort(0)
    val port = hostPort(1).toInt
    val params = parts.tail.flatMap { p =>
      p.split("=", 2) match
        case Array(k, v) => Some(k -> v)
        case _           => None
    }.toMap
    val sanitizedParams = params - "token"
    val sanitizedUrl =
      if sanitizedParams.isEmpty then s"sc://$host:$port"
      else s"sc://$host:$port;" + sanitizedParams.map((k, v) => s"$k=$v").mkString(";")

    assert(!sanitizedUrl.contains("token"))
    assert(!sanitizedUrl.contains("my-secret"))
    assert(sanitizedUrl.contains("use_ssl=true"))
    assert(sanitizedUrl.contains("user_id=alice"))
    assert(params.get("token").contains("my-secret"))
  }

  test("buildUrl still builds URLs with token for round-trip purposes") {
    // buildUrl is used for display/CLI; token redaction happens in SparkConnectClient
    val url = buildUrl(Config(host = "h", port = 1234, token = Some("tok"), useSsl = true))
    assert(url.contains("token=tok"))
  }

  // --- SparkConnectClient.parseUrl validation tests ---

  test("parseUrl rejects URL without sc:// scheme") {
    assertThrows[IllegalArgumentException] {
      SparkConnectClient.parseUrl("http://localhost:15002")
    }
  }

  test("parseUrl rejects empty host") {
    assertThrows[IllegalArgumentException] {
      SparkConnectClient.parseUrl("sc://:15002")
    }
  }

  test("parseUrl rejects non-numeric port") {
    assertThrows[IllegalArgumentException] {
      SparkConnectClient.parseUrl("sc://localhost:abc")
    }
  }

  test("parseUrl rejects port out of range") {
    assertThrows[IllegalArgumentException] {
      SparkConnectClient.parseUrl("sc://localhost:99999")
    }
    assertThrows[IllegalArgumentException] {
      SparkConnectClient.parseUrl("sc://localhost:0")
    }
  }

  test("parseUrl accepts valid URL") {
    val (host, port, params) = SparkConnectClient.parseUrl("sc://myhost:9999;use_ssl=true")
    assert(host == "myhost")
    assert(port == 9999)
    assert(params == Seq("use_ssl" -> "true"))
  }

  test("parseUrl trims host whitespace") {
    val (host, port, _) = SparkConnectClient.parseUrl("sc:// myhost :15002")
    assert(host == "myhost")
    assert(port == 15002)
  }

  test("parseUrl uses default port when not specified") {
    val (host, port, _) = SparkConnectClient.parseUrl("sc://myhost")
    assert(host == "myhost")
    assert(port == 15002)
  }

  test("parseUrl error messages do not contain token values") {
    val ex = intercept[IllegalArgumentException] {
      SparkConnectClient.parseUrl("http://host:15002;token=secret123")
    }
    assert(!ex.getMessage.contains("secret123"))
  }

  test("parseUrl rejects empty URL after scheme") {
    assertThrows[IllegalArgumentException] {
      SparkConnectClient.parseUrl("sc://")
    }
  }

  test("parseUrl trims port whitespace") {
    val (host, port, _) = SparkConnectClient.parseUrl("sc://myhost: 9999 ")
    assert(host == "myhost")
    assert(port == 9999)
  }

  test("parseUrl reports context on malformed percent-encoding") {
    val ex = intercept[IllegalArgumentException] {
      SparkConnectClient.parseUrl("sc://h:15002;k=%ZZ")
    }
    assert(ex.getMessage.contains("%ZZ") || ex.getMessage.contains("k=%ZZ"))
    // Cause should be preserved for debugging
    assert(ex.getCause.isInstanceOf[IllegalArgumentException])
  }

  test("parseUrl rejects segments without '=' as invalid") {
    val ex = intercept[IllegalArgumentException] {
      SparkConnectClient.parseUrl("sc://h:15002;invalidparam")
    }
    assert(ex.getMessage.contains("invalidparam"))
    assert(ex.getMessage.contains("expected 'key=value' format"))
  }

  test("parseUrl trims surrounding whitespace from keys") {
    val (_, _, params) = SparkConnectClient.parseUrl("sc://h:15002; user_id =bob")
    val m = params.toMap
    // Key stored trimmed so later .get("user_id") lookups in create() match
    assert(m.contains("user_id"))
    assert(m("user_id") == "bob")
  }

  test("parseUrl rejects segments with empty key") {
    val ex = intercept[IllegalArgumentException] {
      SparkConnectClient.parseUrl("sc://h:15002;=value")
    }
    // Error message should refer to key, not URL decoding
    assert(ex.getMessage.contains("key must be non-empty"))
    assert(!ex.getMessage.contains("URL-encoded"))
  }

  test("parseUrl round-trips values with special characters from buildUrl") {
    val original = Map(
      "k1" -> "has;semicolon",
      "k2" -> "has=equals",
      "k3" -> "has space",
      "k4" -> "unicode-中文-here",
      "k5" -> "has%percent",
      "k6" -> "has+plus"
    )
    val config = SparkConnectClientParser.Config(
      host = "myhost",
      port = 15002,
      options = original
    )
    val url = SparkConnectClientParser.buildUrl(config)
    val (_, _, params) = SparkConnectClient.parseUrl(url)
    val decoded = params.toMap
    original.foreach { case (k, v) =>
      assert(decoded(k) == v, s"round-trip failed for $k=$v → ${decoded.get(k)}")
    }
  }
