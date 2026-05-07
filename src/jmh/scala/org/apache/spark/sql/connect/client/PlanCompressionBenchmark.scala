package org.apache.spark.sql.connect.client

import com.github.luben.zstd.Zstd
import org.apache.spark.connect.proto.*
import org.openjdk.jmh.annotations.*
import org.openjdk.jmh.infra.Blackhole

import java.util.concurrent.TimeUnit

/** JMH benchmarks for ZSTD plan compression throughput and size reduction.
  *
  * Measures compression/decompression latency and reports compression ratio
  * for protobuf-serialized execution plans of varying sizes.
  */
@BenchmarkMode(Array(Mode.Throughput, Mode.AverageTime))
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 3, time = 2, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 2, timeUnit = TimeUnit.SECONDS)
@Fork(1)
@State(Scope.Benchmark)
class PlanCompressionBenchmark:

  @Param(Array("1024", "10240", "102400", "1048576"))
  var planSizeBytes: Int = 0

  // Pre-built plan bytes and their compressed forms
  private var planBytes: Array[Byte] = _
  private var compressedBytes: Array[Byte] = _
  private var compressionRatio: Double = _

  @Setup(Level.Trial)
  def setup(): Unit =
    // Generate a realistic plan-like protobuf payload of the target size.
    // We build a plan with repeated project expressions to reach the desired size.
    planBytes = generatePlanBytes(planSizeBytes)
    compressedBytes = Zstd.compress(planBytes)
    compressionRatio = compressedBytes.length.toDouble / planBytes.length
    System.out.println(
      f"[Setup] planSize=${planBytes.length}%,d bytes → compressed=${compressedBytes.length}%,d bytes " +
        f"(ratio=$compressionRatio%.4f, saved=${(1 - compressionRatio) * 100}%.1f%%)"
    )

  @Benchmark
  def compress(bh: Blackhole): Unit =
    bh.consume(Zstd.compress(planBytes))

  @Benchmark
  def decompress(bh: Blackhole): Unit =
    bh.consume(Zstd.decompress(compressedBytes, planBytes.length))

  @Benchmark
  def compressAndWrapProto(bh: Blackhole): Unit =
    val compressed = Zstd.compress(planBytes)
    val compOp = Plan.CompressedOperation.newBuilder()
      .setData(com.google.protobuf.ByteString.copyFrom(compressed))
      .setOpType(Plan.CompressedOperation.OpType.OP_TYPE_RELATION)
      .setCompressionCodec(CompressionCodec.COMPRESSION_CODEC_ZSTD)
      .build()
    bh.consume(Plan.newBuilder().setCompressedOperation(compOp).build())

  /** Generate protobuf plan bytes of approximately the target size by
    * building a Plan with many projected columns.
    */
  private def generatePlanBytes(targetSize: Int): Array[Byte] =
    // Start with a base relation
    val baseRelation = Relation.newBuilder()
      .setCommon(RelationCommon.newBuilder().setPlanId(0).build())
      .setLocalRelation(LocalRelation.getDefaultInstance)
      .build()

    // Add project expressions until we reach the target size
    var plan = Plan.newBuilder().setRoot(baseRelation).build()
    var currentSize = plan.getRoot.toByteArray.length
    var colIndex = 0

    while currentSize < targetSize do
      val expressions = (0 until 50).map { i =>
        val colName = s"column_${colIndex + i}_with_a_longer_name_for_realistic_size"
        Expression.newBuilder()
          .setUnresolvedAttribute(
            Expression.UnresolvedAttribute.newBuilder()
              .setUnparsedIdentifier(colName)
              .build()
          ).build()
      }
      colIndex += 50

      val projectRel = Relation.newBuilder()
        .setCommon(RelationCommon.newBuilder().setPlanId(colIndex.toLong).build())
        .setProject(
          Project.newBuilder()
            .setInput(plan.getRoot)
            .addAllExpressions(java.util.List.copyOf(
              scala.jdk.CollectionConverters.SeqHasAsJava(expressions).asJava
            ))
            .build()
        ).build()

      plan = Plan.newBuilder().setRoot(projectRel).build()
      currentSize = plan.getRoot.toByteArray.length

    plan.getRoot.toByteArray
