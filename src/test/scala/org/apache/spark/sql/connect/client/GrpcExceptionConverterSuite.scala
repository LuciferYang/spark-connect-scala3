package org.apache.spark.sql.connect.client

import com.google.protobuf.Any as ProtoAny
import com.google.rpc.{ErrorInfo, Status as RpcStatus}
import io.grpc.{Status, StatusRuntimeException}
import io.grpc.protobuf.StatusProto
import org.apache.spark.connect.proto.FetchErrorDetailsResponse
import org.apache.spark.sql.SparkException
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class GrpcExceptionConverterSuite extends AnyFunSuite with Matchers:

  private def makeExceptionWithErrorInfo(
      code: Int,
      message: String,
      errorClass: String = "",
      sqlState: String = ""
  ): StatusRuntimeException =
    val errorInfo = ErrorInfo.newBuilder()
      .setReason("org.apache.spark.SparkException")
      .setDomain("org.apache.spark")
      .putMetadata("errorClass", errorClass)
      .putMetadata("sqlState", sqlState)
      .putMetadata("message", message)
      .build()
    val rpcStatus = RpcStatus.newBuilder()
      .setCode(code)
      .setMessage(message)
      .addDetails(ProtoAny.pack(errorInfo))
      .build()
    StatusProto.toStatusRuntimeException(rpcStatus)

  test("convert wraps StatusRuntimeException without ErrorInfo") {
    val sre = new StatusRuntimeException(Status.INTERNAL.withDescription("boom"))
    val ex = intercept[SparkException] {
      GrpcExceptionConverter.convert(throw sre)
    }
    ex.getMessage shouldBe "boom"
    ex.getCause shouldBe sre
    ex.errorClass shouldBe None
    ex.sqlState shouldBe None
  }

  test("convert extracts errorClass and sqlState from ErrorInfo") {
    val sre = makeExceptionWithErrorInfo(
      code = Status.Code.INTERNAL.value(),
      message = "Table not found",
      errorClass = "TABLE_OR_VIEW_NOT_FOUND",
      sqlState = "42P01"
    )
    val ex = intercept[SparkException] {
      GrpcExceptionConverter.convert(throw sre)
    }
    ex.getMessage shouldBe "Table not found"
    ex.errorClass shouldBe Some("TABLE_OR_VIEW_NOT_FOUND")
    ex.sqlState shouldBe Some("42P01")
    ex.getCause shouldBe sre
  }

  test("convert passes through non-StatusRuntimeException") {
    val re = RuntimeException("not grpc")
    val thrown = intercept[RuntimeException] {
      GrpcExceptionConverter.convert(throw re)
    }
    thrown shouldBe re
  }

  test("convert returns value when no exception") {
    val result = GrpcExceptionConverter.convert(42)
    result shouldBe 42
  }

  test("toSparkException with empty ErrorInfo metadata") {
    val errorInfo = ErrorInfo.newBuilder()
      .setReason("org.apache.spark.SparkException")
      .setDomain("org.apache.spark")
      .build()
    val rpcStatus = RpcStatus.newBuilder()
      .setCode(Status.Code.INTERNAL.value())
      .setMessage("server error")
      .addDetails(ProtoAny.pack(errorInfo))
      .build()
    val sre = StatusProto.toStatusRuntimeException(rpcStatus)
    val ex = GrpcExceptionConverter.toSparkException(sre)
    ex.errorClass shouldBe None
    ex.sqlState shouldBe None
    // message falls back to description
    ex.getMessage shouldBe "server error"
  }

  // ---------- FetchErrorDetails tests ----------

  test("convert with fetchDetails reconstructs exception from response") {
    val sre = makeExceptionWithErrorId(
      code = Status.Code.INTERNAL.value(),
      message = "Division by zero",
      errorId = "err-123"
    )
    val fetchDetails: String => Option[FetchErrorDetailsResponse] = { errorId =>
      errorId shouldBe "err-123"
      Some(buildFetchErrorDetailsResponse(
        rootErrorIdx = 0,
        errors = Seq(
          buildError(
            message = "Division by zero",
            errorClass = Some("DIVIDE_BY_ZERO"),
            sqlState = Some("22012"),
            params = Map("config" -> "spark.sql.ansi.enabled")
          )
        )
      ))
    }
    val ex = intercept[SparkException] {
      GrpcExceptionConverter.convert(throw sre, fetchDetails)
    }
    ex.getMessage shouldBe "Division by zero"
    ex.errorClass shouldBe Some("DIVIDE_BY_ZERO")
    ex.sqlState shouldBe Some("22012")
    ex.messageParameters shouldBe Map("config" -> "spark.sql.ansi.enabled")
  }

  test("convert with fetchDetails builds exception chain from cause_idx") {
    val sre = makeExceptionWithErrorId(
      code = Status.Code.INTERNAL.value(),
      message = "root error",
      errorId = "err-456"
    )
    val fetchDetails: String => Option[FetchErrorDetailsResponse] = { _ =>
      Some(buildFetchErrorDetailsResponse(
        rootErrorIdx = 0,
        errors = Seq(
          buildError(message = "root error", causeIdx = Some(1)),
          buildError(
            message = "cause error",
            errorClass = Some("INTERNAL_ERROR"),
            sqlState = Some("XX000")
          )
        )
      ))
    }
    val ex = intercept[SparkException] {
      GrpcExceptionConverter.convert(throw sre, fetchDetails)
    }
    ex.getMessage shouldBe "root error"
    // The cause should be a SparkException with the cause error details
    ex.getCause shouldBe a[SparkException]
    val cause = ex.getCause.asInstanceOf[SparkException]
    cause.getMessage shouldBe "cause error"
    cause.errorClass shouldBe Some("INTERNAL_ERROR")
    cause.sqlState shouldBe Some("XX000")
  }

  test("convert with fetchDetails includes server-side stack trace") {
    val sre = makeExceptionWithErrorId(
      code = Status.Code.INTERNAL.value(),
      message = "with trace",
      errorId = "err-789"
    )
    val fetchDetails: String => Option[FetchErrorDetailsResponse] = { _ =>
      Some(buildFetchErrorDetailsResponse(
        rootErrorIdx = 0,
        errors = Seq(
          buildError(
            message = "with trace",
            stackTrace = Seq(
              ("org.apache.spark.sql.Foo", "bar", Some("Foo.scala"), 42)
            )
          )
        )
      ))
    }
    val ex = intercept[SparkException] {
      GrpcExceptionConverter.convert(throw sre, fetchDetails)
    }
    // Server-side stack trace should be prepended
    val trace = ex.getStackTrace
    trace.head.getClassName shouldBe "org.apache.spark.sql.Foo"
    trace.head.getMethodName shouldBe "bar"
    trace.head.getFileName shouldBe "Foo.scala"
    trace.head.getLineNumber shouldBe 42
  }

  test("convert with fetchDetails falls back when fetchDetails fails") {
    val sre = makeExceptionWithErrorInfo(
      code = Status.Code.INTERNAL.value(),
      message = "fallback test",
      errorClass = "SOME_ERROR",
      sqlState = "42000"
    )
    // Add errorId to metadata
    val fetchDetails: String => Option[FetchErrorDetailsResponse] = _ => None
    val ex = intercept[SparkException] {
      GrpcExceptionConverter.convert(throw sre, fetchDetails)
    }
    // Should fall back to ErrorInfo-based extraction
    ex.getMessage shouldBe "fallback test"
    ex.errorClass shouldBe Some("SOME_ERROR")
  }

  // ---------- helpers for FetchErrorDetails tests ----------

  private def makeExceptionWithErrorId(
      code: Int,
      message: String,
      errorId: String
  ): StatusRuntimeException =
    val errorInfo = ErrorInfo.newBuilder()
      .setReason("org.apache.spark.SparkException")
      .setDomain("org.apache.spark")
      .putMetadata("errorId", errorId)
      .putMetadata("message", message)
      .build()
    val rpcStatus = RpcStatus.newBuilder()
      .setCode(code)
      .setMessage(message)
      .addDetails(ProtoAny.pack(errorInfo))
      .build()
    StatusProto.toStatusRuntimeException(rpcStatus)

  private def buildFetchErrorDetailsResponse(
      rootErrorIdx: Int,
      errors: Seq[FetchErrorDetailsResponse.Error]
  ): FetchErrorDetailsResponse =
    val builder = FetchErrorDetailsResponse.newBuilder()
      .setRootErrorIdx(rootErrorIdx)
    errors.foreach(builder.addErrors)
    builder.build()

  private def buildError(
      message: String,
      errorClass: Option[String] = None,
      sqlState: Option[String] = None,
      params: Map[String, String] = Map.empty,
      causeIdx: Option[Int] = None,
      stackTrace: Seq[(String, String, Option[String], Int)] = Seq.empty
  ): FetchErrorDetailsResponse.Error =
    val builder = FetchErrorDetailsResponse.Error.newBuilder()
      .setMessage(message)

    if errorClass.isDefined || sqlState.isDefined || params.nonEmpty then
      val stBuilder = FetchErrorDetailsResponse.SparkThrowable.newBuilder()
      errorClass.foreach(stBuilder.setErrorClass)
      sqlState.foreach(stBuilder.setSqlState)
      params.foreach((k, v) => stBuilder.putMessageParameters(k, v))
      builder.setSparkThrowable(stBuilder.build())

    causeIdx.foreach(builder.setCauseIdx)

    stackTrace.foreach { (cls, method, file, line) =>
      val steBuilder = FetchErrorDetailsResponse.StackTraceElement.newBuilder()
        .setDeclaringClass(cls)
        .setMethodName(method)
        .setLineNumber(line)
      file.foreach(steBuilder.setFileName)
      builder.addStackTrace(steBuilder.build())
    }

    builder.build()
