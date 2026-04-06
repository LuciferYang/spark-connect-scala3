package org.apache.spark.sql.connect.client

import com.google.protobuf.ByteString
import io.grpc.stub.StreamObserver
import org.apache.spark.connect.proto.*
import org.apache.spark.sql.Artifact

import java.nio.file.{Files, LinkOption, Path, Paths}
import java.util.concurrent.CopyOnWriteArrayList
import java.util.zip.{CRC32, CheckedInputStream}
import scala.collection.mutable
import scala.concurrent.{Await, Promise}
import scala.concurrent.duration.Duration
import scala.jdk.CollectionConverters.*
import scala.util.control.NonFatal

/** Discovers class files to upload. Implementations can monitor REPL output directories or
  * application class paths.
  */
trait ClassFinder:
  def findClasses(): Iterator[Artifact]

/** A `ClassFinder` that walks a directory tree and collects `.class` files.
  *
  * {{{
  *   val finder = ClassDirFinder("/path/to/classes")
  *   spark.registerClassFinder(finder)
  * }}}
  */
class ClassDirFinder(rootDir: Path) extends ClassFinder:
  require(rootDir.isAbsolute, s"Root dir must be absolute: $rootDir")
  require(Files.isDirectory(rootDir), s"Not a directory: $rootDir")

  override def findClasses(): Iterator[Artifact] =
    val stream = Files.walk(rootDir)
    try
      stream
        .filter(p =>
          Files.isRegularFile(p, LinkOption.NOFOLLOW_LINKS) && p.toString.endsWith(".class")
        )
        .map[Artifact](p => Artifact.newClassArtifact(rootDir.relativize(p), Artifact.LocalFile(p)))
        .iterator()
        .asScala
        .toVector // eagerly collect so stream can be closed
        .iterator
    finally stream.close()

/** Manages uploading artifacts (class files, JARs) to the Spark Connect server via the
  * `AddArtifacts` client-streaming gRPC.
  *
  * Call [[addArtifact]] or [[addClassDir]] to upload explicitly. Register a `ClassFinder` via
  * [[registerClassFinder]] for automatic upload before every plan execution.
  */
final class ArtifactManager private[client] (
    private val sessionId: String,
    private val userId: String,
    private val asyncStub: SparkConnectServiceGrpc.SparkConnectServiceStub
):

  // 32 KiB chunk size (gRPC recommendation)
  private val CHUNK_SIZE = 32 * 1024

  private val classFinders = CopyOnWriteArrayList[ClassFinder]()

  /** Register a `ClassFinder` whose classes are uploaded before every plan execution. */
  def registerClassFinder(finder: ClassFinder): Unit = classFinders.add(finder)

  /** Upload a JAR or class file from the local filesystem. */
  def addArtifact(path: String): Unit =
    val p = Paths.get(path)
    val artifact =
      Artifact.newFromExtension(p.getFileName.toString, p.getFileName, Artifact.LocalFile(p))
    addArtifacts(Seq(artifact))

  /** Upload in-memory bytes as an artifact with the given target path. */
  def addArtifact(bytes: Array[Byte], target: String): Unit =
    val targetPath = Paths.get(target)
    val artifact =
      Artifact.newFromExtension(
        targetPath.getFileName.toString,
        targetPath,
        Artifact.InMemory(bytes)
      )
    addArtifacts(Seq(artifact))

  /** Upload all `.class` files under a directory, preserving the directory structure. */
  def addClassDir(base: Path): Unit =
    if !Files.isDirectory(base) then return
    val builder = Seq.newBuilder[Artifact]
    val stream = Files.walk(base)
    try
      stream.forEach { path =>
        if Files.isRegularFile(path) && path.toString.endsWith(".class") then
          builder += Artifact.newClassArtifact(base.relativize(path), Artifact.LocalFile(path))
      }
    finally stream.close()
    addArtifacts(builder.result())

  /** Upload all class files discovered by registered `ClassFinder`s. Called automatically before
    * every plan execution.
    */
  private[client] def uploadAllClassFileArtifacts(): Unit =
    addArtifacts(classFinders.asScala.flatMap(_.findClasses()))

  /** Upload a collection of artifacts via the `AddArtifacts` client-streaming gRPC. */
  private[sql] def addArtifacts(artifacts: Iterable[Artifact]): Unit =
    if artifacts.isEmpty then return
    addArtifactsImpl(artifacts)

  private def addArtifactsImpl(artifacts: Iterable[Artifact]): Unit =
    val promise = Promise[Seq[AddArtifactsResponse.ArtifactSummary]]()
    val responseHandler = new StreamObserver[AddArtifactsResponse]:
      private val summaries = mutable.Buffer.empty[AddArtifactsResponse.ArtifactSummary]
      override def onNext(v: AddArtifactsResponse): Unit =
        v.getArtifactsList.forEach(s => summaries += s)
      override def onError(throwable: Throwable): Unit =
        promise.failure(throwable)
      override def onCompleted(): Unit =
        promise.success(summaries.toSeq)

    val stream = asyncStub.addArtifacts(responseHandler)
    val currentBatch = mutable.Buffer.empty[Artifact]
    var currentBatchSize = 0L

    def writeBatch(): Unit =
      addBatchedArtifacts(currentBatch.toSeq, stream)
      currentBatch.clear()
      currentBatchSize = 0

    try
      artifacts.iterator.foreach { artifact =>
        val size = artifact.size
        if size > CHUNK_SIZE then
          if currentBatch.nonEmpty then writeBatch()
          addChunkedArtifact(artifact, stream)
        else
          if currentBatchSize + size > CHUNK_SIZE then writeBatch()
          currentBatch += artifact
          currentBatchSize += size
      }
      if currentBatch.nonEmpty then writeBatch()
      stream.onCompleted()
    catch
      case NonFatal(e) =>
        stream.onError(e)
        throw e

    Await.result(promise.future, Duration.Inf)

  /** Send small artifacts in a single batched request. */
  private def addBatchedArtifacts(
      artifacts: Seq[Artifact],
      stream: StreamObserver[AddArtifactsRequest]
  ): Unit =
    val builder = AddArtifactsRequest.newBuilder()
      .setSessionId(sessionId)
      .setUserContext(UserContext.newBuilder().setUserId(userId).build())

    artifacts.foreach { artifact =>
      val in = CheckedInputStream(artifact.storage.stream, CRC32())
      try
        val data = AddArtifactsRequest.ArtifactChunk.newBuilder()
          .setData(ByteString.readFrom(in))
          .setCrc(in.getChecksum.getValue)
        builder.getBatchBuilder
          .addArtifactsBuilder()
          .setName(artifact.path.toString)
          .setData(data)
          .build()
      finally in.close()
    }
    stream.onNext(builder.build())

  /** Send a large artifact as multiple chunked requests. */
  private def addChunkedArtifact(
      artifact: Artifact,
      stream: StreamObserver[AddArtifactsRequest]
  ): Unit =
    val builder = AddArtifactsRequest.newBuilder()
      .setSessionId(sessionId)
      .setUserContext(UserContext.newBuilder().setUserId(userId).build())

    val in = CheckedInputStream(artifact.storage.stream, CRC32())
    try
      val chunkBuilder = AddArtifactsRequest.ArtifactChunk.newBuilder()
      var dataChunk = readNextChunk(in)
      val numChunks = (artifact.size + (CHUNK_SIZE - 1)) / CHUNK_SIZE

      // First message: BeginChunkedArtifact
      builder.getBeginChunkBuilder
        .setName(artifact.path.toString)
        .setTotalBytes(artifact.size)
        .setNumChunks(numChunks)
        .setInitialChunk(chunkBuilder.setData(dataChunk).setCrc(in.getChecksum.getValue))
      stream.onNext(builder.build())
      in.getChecksum.reset()
      builder.clearBeginChunk()

      // Subsequent messages: ArtifactChunk
      dataChunk = readNextChunk(in)
      while !dataChunk.isEmpty do
        chunkBuilder.setData(dataChunk).setCrc(in.getChecksum.getValue)
        builder.setChunk(chunkBuilder.build())
        stream.onNext(builder.build())
        in.getChecksum.reset()
        builder.clearChunk()
        dataChunk = readNextChunk(in)
    finally in.close()

  private def readNextChunk(in: java.io.InputStream): ByteString =
    val buf = new Array[Byte](CHUNK_SIZE)
    var bytesRead = 0
    var count = 0
    while count != -1 && bytesRead < CHUNK_SIZE do
      count = in.read(buf, bytesRead, CHUNK_SIZE - bytesRead)
      if count != -1 then bytesRead += count
    if bytesRead == 0 then ByteString.empty()
    else ByteString.copyFrom(buf, 0, bytesRead)
