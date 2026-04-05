# Spark Connect Scala 3 REPL Guide

Spark Connect Scala 3 提供了一个基于 [Ammonite](https://ammonite.io/) 的交互式 REPL，可以直接连接到 Spark Connect 服务器进行数据探索和开发。

## 前置条件

| 组件 | 要求 |
|------|------|
| JDK | 17+ |
| SBT | 1.10+ |
| Scala | 3.3.7 LTS（自动由 SBT 管理） |
| Spark Connect Server | 4.0.2+，已启动并监听 |

启动 Spark Connect Server：

```bash
$SPARK_HOME/sbin/start-connect-server.sh
```

## 启动 REPL

### 方式一：默认连接（localhost:15002）

```bash
build/sbt run
```

### 方式二：通过 `--remote` 指定完整 URL

```bash
build/sbt "run --remote sc://myhost:15002"
```

### 方式三：通过 `--host` / `--port` 分别指定

```bash
build/sbt "run --host myhost --port 15002"
```

### 方式四：通过环境变量

```bash
SPARK_REMOTE=sc://myhost:15002 build/sbt run
```

### 连接优先级

当多种方式同时存在时，按以下优先级选取：

1. `--remote` CLI 参数（最高）
2. `SPARK_REMOTE` 环境变量
3. 从 `--host` / `--port` 等参数构建 URL（默认 `sc://localhost:15002`）

## CLI 参数一览

| 参数 | 值 | 说明 |
|------|----|------|
| `--remote REMOTE` | `sc://host:port` | Spark Connect Server 的完整 URI |
| `--host HOST` | 主机名 | 服务器地址，默认 `localhost` |
| `--port PORT` | 端口号 | 服务器端口，默认 `15002` |
| `--use_ssl` | （无值） | 启用 SSL 连接 |
| `--token TOKEN` | 字符串 | 认证 token |
| `--user_id USER_ID` | 字符串 | 连接用户 ID |
| `--user_name USER_NAME` | 字符串 | 连接用户名 |
| `--session_id SESSION_ID` | 字符串 | 指定 session ID（默认自动生成） |
| `--option KEY=VALUE` | 键值对 | 传递 Spark 配置项，可多次使用 |

示例——带认证和配置项：

```bash
build/sbt "run --host spark.example.com --port 15002 --use_ssl --token my-secret --option spark.sql.shuffle.partitions=10"
```

## REPL 内预加载的内容

启动后，REPL 自动执行以下 predef 代码：

```scala
import org.apache.spark.sql.functions.*    // 542 个内置函数
import org.apache.spark.sql.implicits.*    // $"col"、Seq[T].toDS/toDF 等
given SparkSession = spark                  // 隐式 SparkSession（用于 toDS/toDF）
spark.registerClassFinder(...)             // 自动上传 REPL 中定义的类到服务器
```

可直接使用的对象：

| 名称 | 类型 | 说明 |
|------|------|------|
| `spark` | `SparkSession` | 已连接的 Spark 会话 |
| `repl` | Ammonite REPL API | Ammonite 内部 REPL 对象 |

## 使用示例

### SQL 查询

```scala
scala> spark.sql("SELECT 1 + 1 AS result").show()
+------+
|result|
+------+
|     2|
+------+

scala> spark.sql("SHOW DATABASES").show()
```

### DataFrame 操作

```scala
scala> val df = spark.range(10)
scala> df.select(col("id"), (col("id") * 2).as("doubled")).show()
+---+-------+
| id|doubled|
+---+-------+
|  0|      0|
|  1|      2|
|  2|      4|
...

scala> df.filter(col("id") > 5).orderBy(col("id").desc).show()
```

### 使用 $"col" 语法

```scala
scala> val df = spark.range(10)
scala> df.filter($"id" > 5).select($"id", ($"id" % 3).as("mod3")).show()
```

### 读取数据

```scala
scala> val df = spark.read.format("parquet").load("/path/to/data.parquet")
scala> df.printSchema()
scala> df.describe().show()
```

### 写入数据

```scala
scala> spark.range(100)
     |   .withColumn("name", lit("test"))
     |   .write
     |   .mode("overwrite")
     |   .parquet("/tmp/output")
```

### 创建临时表并查询

```scala
scala> spark.range(10).createOrReplaceTempView("numbers")
scala> spark.sql("SELECT id, id * id AS squared FROM numbers WHERE id > 3").show()
```

### Dataset[T] — 类型安全操作

```scala
scala> case class Person(name: String, age: Int) derives Encoder

scala> val people = Seq(Person("Alice", 30), Person("Bob", 25)).toDS
scala> people.filter(_.age > 26).show()
+-----+---+
| name|age|
+-----+---+
|Alice| 30|
+-----+---+
```

> 注意：`derives Encoder` 是 Scala 3 的编译期派生，无需 `import spark.implicits._` 来获取 Encoder（但 `toDS` 方法仍需要 `given SparkSession`，已通过 predef 提供）。

### 聚合

```scala
scala> val df = spark.range(100)
scala> df.groupBy(($"id" % 10).as("group"))
     |   .agg(count("*").as("cnt"), sum("id").as("total"))
     |   .orderBy($"group")
     |   .show()
```

### UDF（用户自定义函数）

在 REPL 中定义的类和函数会通过 `AmmoniteClassFinder` 自动上传到服务器：

```scala
scala> val double = udf((x: Int) => x * 2)
scala> spark.range(5).select(col("id"), double(col("id")).as("doubled")).show()
+---+-------+
| id|doubled|
+---+-------+
|  0|      0|
|  1|      2|
|  2|      4|
|  3|      6|
|  4|      8|
+---+-------+
```

注册为全局可用的 SQL 函数：

```scala
scala> spark.udf.register("my_double", (x: Int) => x * 2)
scala> spark.sql("SELECT id, my_double(id) AS doubled FROM range(5)").show()
```

### Window 函数

```scala
scala> import org.apache.spark.sql.expressions.Window

scala> val w = Window.orderBy($"id")
scala> spark.range(5)
     |   .withColumn("running_sum", sum($"id").over(w))
     |   .show()
```

### Structured Streaming（交互式探索）

```scala
scala> val stream = spark.readStream
     |   .format("rate")
     |   .option("rowsPerSecond", "5")
     |   .load()

scala> val query = stream.writeStream
     |   .format("console")
     |   .outputMode("append")
     |   .start()

scala> query.isActive   // true
scala> query.stop()
```

### Catalog 操作

```scala
scala> spark.catalog.listDatabases().show()
scala> spark.catalog.listTables().show()
scala> spark.catalog.tableExists("my_table")
```

### 运行时配置

```scala
scala> spark.conf.get("spark.sql.shuffle.partitions")
scala> spark.conf.set("spark.sql.shuffle.partitions", "100")
```

## 与上游 Spark REPL 的差异

| 特性 | 上游 Spark REPL | SC3 REPL |
|------|----------------|----------|
| Scala 版本 | 2.13 | 3.3.7 |
| 通配符导入 | `import functions._` | `import functions.*` |
| 隐式 Session | `import spark.implicits._` | `given SparkSession = spark` |
| Encoder 派生 | 运行时反射 | 编译期 `derives Encoder` |
| `ExtendedCodeClassWrapper` | 有（注册 OuterScopes） | 无（使用 `CodeClassWrapper`） |
| 内嵌 Spark Server | 支持 `withLocalConnectServer` | 不支持（纯客户端） |
| `spark-shell` 脚本 | 有 | 无（通过 `build/sbt run` 启动） |

## 常见问题

### Q: 连接超时 / 无法连接

确认 Spark Connect Server 已启动且端口可达：

```bash
# 检查服务器是否监听
nc -zv localhost 15002

# 启动服务器
$SPARK_HOME/sbin/start-connect-server.sh
```

### Q: REPL 中定义的 case class 在 UDF 中报 ClassNotFoundException

SC3 的 `AmmoniteClassFinder` 会自动将 REPL 中编译的类上传到服务器。如果仍报错：

1. 确认类定义在 REPL 中执行成功（无编译错误）
2. 确认 `spark.registerClassFinder(...)` 在 predef 中已执行（启动时无报错）
3. UDF 中引用的所有类型都需要在 REPL 中定义或在 classpath 上

### Q: 如何退出 REPL？

输入 `exit` 或按 `Ctrl+D`。

### Q: 如何添加外部 JAR？

```scala
scala> spark.addArtifact("/path/to/my-lib.jar")
```

### Q: 如何使用 SSL 连接？

```bash
build/sbt "run --host secure-host --port 15002 --use_ssl --token my-token"
```

或通过 `--remote` URL 参数：

```bash
build/sbt "run --remote sc://secure-host:15002;use_ssl=true;token=my-token"
```
