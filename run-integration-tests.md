# Running Integration Tests

Integration tests require a live Spark Connect server (4.1.x) listening on `sc://localhost:15002`.

## Prerequisites

Start the Spark Connect server:

```bash
$SPARK_HOME/sbin/start-connect-server.sh
```

## Run all integration tests

```bash
SBT_OPTS="-Dsun.net.client.defaultConnectTimeout=10000 -Dsun.net.client.defaultReadTimeout=60000 -Djava.net.preferIPv4Stack=true" \
  build/sbt 'set Test / testOptions := Seq()' 'testOnly *IntegrationSuite' \
  | tee /tmp/sc3-integration.log
```

The `set Test / testOptions := Seq()` clears the default `IntegrationTest` tag exclusion so tests tagged `@IntegrationTest` actually run.

## Run a single suite

```bash
SBT_OPTS="-Dsun.net.client.defaultConnectTimeout=10000 -Dsun.net.client.defaultReadTimeout=60000 -Djava.net.preferIPv4Stack=true" \
  build/sbt 'set Test / testOptions := Seq()' 'testOnly *DataFrameIntegrationSuite'
```

## Notes on the SBT_OPTS flags

- `sun.net.client.defaultConnectTimeout=10000` — 10s socket connect timeout. Without this, sbt/Coursier hangs forever on unreachable IPs from DNS round-robin.
- `sun.net.client.defaultReadTimeout=60000` — 60s read timeout for slow downloads.
- `java.net.preferIPv4Stack=true` — forces IPv4. Mixed IPv4/IPv6 stacks have caused stuck connections in this environment.

## Coursier mirror configuration

For environments where `repo1.maven.org` is slow or blocked, configure a Coursier mirror at:

```
~/Library/Application Support/Coursier/config/mirror.properties
```

Contents:

```properties
central.from=https://repo1.maven.org/maven2,https://repo.maven.apache.org/maven2
central.to=https://maven-central.storage-download.googleapis.com/maven2
```

This is per-user and does **not** affect the Maven release pipeline (`publishTo` is unchanged).
