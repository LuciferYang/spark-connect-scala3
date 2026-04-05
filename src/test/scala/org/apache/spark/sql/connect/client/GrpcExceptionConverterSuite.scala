package org.apache.spark.sql.connect.client

import com.google.protobuf.Any as ProtoAny
import com.google.rpc.{ErrorInfo, Status as RpcStatus}
import io.grpc.{Status, StatusRuntimeException}
import io.grpc.protobuf.StatusProto
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
