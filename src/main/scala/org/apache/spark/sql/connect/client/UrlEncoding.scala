package org.apache.spark.sql.connect.client

import java.net.{URLDecoder, URLEncoder}
import java.nio.charset.StandardCharsets

/** URL encoding/decoding utility for `sc://` connection URL parameters.
  *
  * Both `SparkConnectClientParser.buildUrl` (the CLI parser) and `SparkConnectClient.parseUrl` /
  * `buildSanitizedUrl` (the runtime URL lifecycle) must agree on the same
  * `application/x-www-form-urlencoded` scheme so that parameter values containing `;`, `=`,
  * spaces, `+`, `%`, or Unicode round-trip losslessly through `buildUrl` → `parseUrl` and
  * `parseUrl` → `buildSanitizedUrl` → `parseUrl`.
  *
  * UTF-8 is fixed via `StandardCharsets.UTF_8` to avoid the `UnsupportedEncodingException` path
  * on the deprecated String-charset overload.
  */
private[client] object UrlEncoding:

  /** URL-encode a value for use in an `sc://host:port;key=value` segment. */
  def encode(s: String): String = URLEncoder.encode(s, StandardCharsets.UTF_8)

  /** URL-decode a value read out of an `sc://host:port;key=value` segment. */
  def decode(s: String): String = URLDecoder.decode(s, StandardCharsets.UTF_8)
