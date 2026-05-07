package org.apache.spark.sql.connect.client

import org.apache.spark.sql.BenchmarkDataGenerator
import org.openjdk.jmh.annotations.*
import org.openjdk.jmh.infra.Blackhole

import java.util.concurrent.TimeUnit

/** JMH benchmarks for Arrow IPC deserialization throughput.
  *
  * Measures `ArrowDeserializer.fromArrowBatch` and `fromArrowBatchWithSchema`
  * across different data types and batch sizes.
  */
@BenchmarkMode(Array(Mode.Throughput))
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 3, time = 2, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 2, timeUnit = TimeUnit.SECONDS)
@Fork(1)
@State(Scope.Benchmark)
class ArrowDeserializerBenchmark:

  @Param(Array("100", "1000", "10000"))
  var numRows: Int = 0

  // Pre-generated Arrow IPC payloads (set up once per param combination)
  private var intData: Array[Byte] = _
  private var stringData: Array[Byte] = _
  private var decimalData: Array[Byte] = _
  private var timestampData: Array[Byte] = _
  private var nestedStructData: Array[Byte] = _
  private var arrayData: Array[Byte] = _
  private var mapData: Array[Byte] = _
  private var mixedData: Array[Byte] = _

  @Setup(Level.Trial)
  def setup(): Unit =
    intData = BenchmarkDataGenerator.generateIntData(numRows)
    stringData = BenchmarkDataGenerator.generateStringData(numRows)
    decimalData = BenchmarkDataGenerator.generateDecimalData(numRows)
    timestampData = BenchmarkDataGenerator.generateTimestampData(numRows)
    nestedStructData = BenchmarkDataGenerator.generateNestedStructData(numRows)
    arrayData = BenchmarkDataGenerator.generateArrayData(numRows)
    mapData = BenchmarkDataGenerator.generateMapData(numRows)
    mixedData = BenchmarkDataGenerator.generateMixedData(numRows)

  @Benchmark
  def deserializeInts(bh: Blackhole): Unit =
    bh.consume(ArrowDeserializer.fromArrowBatch(intData))

  @Benchmark
  def deserializeStrings(bh: Blackhole): Unit =
    bh.consume(ArrowDeserializer.fromArrowBatch(stringData))

  @Benchmark
  def deserializeDecimals(bh: Blackhole): Unit =
    bh.consume(ArrowDeserializer.fromArrowBatch(decimalData))

  @Benchmark
  def deserializeTimestamps(bh: Blackhole): Unit =
    bh.consume(ArrowDeserializer.fromArrowBatch(timestampData))

  @Benchmark
  def deserializeNestedStructs(bh: Blackhole): Unit =
    bh.consume(ArrowDeserializer.fromArrowBatch(nestedStructData))

  @Benchmark
  def deserializeArrays(bh: Blackhole): Unit =
    bh.consume(ArrowDeserializer.fromArrowBatch(arrayData))

  @Benchmark
  def deserializeMaps(bh: Blackhole): Unit =
    bh.consume(ArrowDeserializer.fromArrowBatch(mapData))

  @Benchmark
  def deserializeMixed(bh: Blackhole): Unit =
    bh.consume(ArrowDeserializer.fromArrowBatch(mixedData))

  @Benchmark
  def deserializeMixedWithSchema(bh: Blackhole): Unit =
    bh.consume(ArrowDeserializer.fromArrowBatchWithSchema(mixedData))
