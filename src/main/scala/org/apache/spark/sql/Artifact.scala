package org.apache.spark.sql

import java.io.{ByteArrayInputStream, InputStream}
import java.nio.file.{Files, Path, Paths}

/** An artifact to be uploaded to the Spark Connect server.
  *
  * The `path` is a relative path with a type prefix (e.g. `classes/com/example/Foo.class` or
  * `jars/my-lib.jar`). The `storage` provides the artifact's content.
  */
private[sql] class Artifact(val path: Path, val storage: Artifact.LocalData):
  require(!path.isAbsolute, s"Artifact path must be relative: $path")
  lazy val size: Long = storage.size

private[sql] object Artifact:

  val CLASS_PREFIX: Path = Paths.get("classes")
  val JAR_PREFIX: Path = Paths.get("jars")

  /** Abstraction over artifact content — either a file on disk or in-memory bytes. */
  trait LocalData:
    def size: Long
    def stream: InputStream

  class LocalFile(val filePath: Path) extends LocalData:
    override def size: Long = Files.size(filePath)
    override def stream: InputStream = Files.newInputStream(filePath)

  class InMemory(val data: Array[Byte]) extends LocalData:
    override def size: Long = data.length.toLong
    override def stream: InputStream = ByteArrayInputStream(data)

  def newClassArtifact(targetPath: Path, storage: LocalData): Artifact =
    Artifact(CLASS_PREFIX.resolve(targetPath), storage)

  def newJarArtifact(targetPath: Path, storage: LocalData): Artifact =
    Artifact(JAR_PREFIX.resolve(targetPath), storage)

  def newFromExtension(
      fileName: String,
      targetPath: Path,
      storage: LocalData
  ): Artifact =
    if fileName.endsWith(".jar") then newJarArtifact(targetPath, storage)
    else if fileName.endsWith(".class") then newClassArtifact(targetPath, storage)
    else throw UnsupportedOperationException(s"Unsupported file format: $fileName")
