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
package org.apache.spark.sql.connect.service

import java.util
import java.util.Optional
import java.util.concurrent.atomic.AtomicBoolean

import scala.concurrent.Promise
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._

import com.google.protobuf
import com.google.protobuf.StringValue
import io.grpc.stub.StreamObserver

import org.apache.spark.connect.proto
import org.apache.spark.connect.proto.InterruptResponse
import org.apache.spark.sql.connect.SparkConnectTestUtils
import org.apache.spark.sql.connect.plugin.{InterruptPlugin, SparkConnectPluginRegistry}
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.util.ThreadUtils

/**
 * A test-only base class for InterruptPlugins that echo their request extensions: for each
 * extension containing a StringValue it returns a response extension "{prefix}{value}".
 */
abstract class EchoInterruptPluginBase(prefix: String) extends InterruptPlugin {
  override def prepareInterrupt(
      sessionHolder: SessionHolder,
      request: proto.InterruptRequest): Optional[util.List[protobuf.Any]] = {
    if (request.getExtensionsList.isEmpty) return Optional.empty()
    val result = new util.ArrayList[protobuf.Any]()
    request.getExtensionsList.forEach { ext =>
      if (ext.is(classOf[StringValue])) {
        val value = ext.unpack(classOf[StringValue]).getValue
        result.add(protobuf.Any.pack(StringValue.of(s"$prefix$value")))
      }
    }
    Optional.of(result)
  }
}

class EchoInterruptPlugin extends EchoInterruptPluginBase("request-echo:")

class SecondEchoInterruptPlugin extends EchoInterruptPluginBase("second-request:")

/** A plugin that never handles the request. */
class NoOpInterruptPlugin extends InterruptPlugin {
  override def prepareInterrupt(
      sessionHolder: SessionHolder,
      request: proto.InterruptRequest): Optional[util.List[protobuf.Any]] =
    Optional.empty()
}

/** A plugin that returns a list containing a null element, to test null filtering. */
class NullElementInterruptPlugin extends InterruptPlugin {
  override def prepareInterrupt(
      sessionHolder: SessionHolder,
      request: proto.InterruptRequest): Optional[util.List[protobuf.Any]] = {
    val result = new util.ArrayList[protobuf.Any]()
    result.add(null)
    Optional.of(result)
  }
}

/**
 * Records that it ran (via `prepared`). Throws on a StringValue "boom" extension to exercise the
 * abort-before-dispatch path.
 */
class RecordingInterruptPlugin(prepared: AtomicBoolean) extends InterruptPlugin {
  override def prepareInterrupt(
      sessionHolder: SessionHolder,
      request: proto.InterruptRequest): Optional[util.List[protobuf.Any]] = {
    request.getExtensionsList.forEach { ext =>
      if (ext.is(classOf[StringValue]) && ext.unpack(classOf[StringValue]).getValue == "boom") {
        throw new RuntimeException("malformed recognized extension")
      }
    }
    prepared.set(true)
    Optional.empty()
  }
}

class InterruptHandlerSuite extends SharedSparkSession {

  protected override def afterEach(): Unit = {
    super.afterEach()
    SparkConnectService.sessionManager.invalidateAllSessions()
    SparkConnectPluginRegistry.reset()
  }

  private def sendInterruptAllRequest(
      sessionId: String,
      userId: String,
      requestExtensions: Seq[protobuf.Any] = Seq.empty): InterruptResponse = {
    sendInterruptRequest(
      sessionId,
      userId,
      { builder =>
        builder.setInterruptType(proto.InterruptRequest.InterruptType.INTERRUPT_TYPE_ALL)
        requestExtensions.foreach(builder.addExtensions)
      })
  }

  private def sendInterruptRequest(
      sessionId: String,
      userId: String,
      customize: proto.InterruptRequest.Builder => Unit): InterruptResponse = {
    val userContext = proto.UserContext.newBuilder().setUserId(userId).build()
    val requestBuilder = proto.InterruptRequest
      .newBuilder()
      .setUserContext(userContext)
      .setSessionId(sessionId)
    customize(requestBuilder)

    val responseObserver = new InterruptResponseObserver()
    new SparkConnectInterruptHandler(responseObserver).handle(requestBuilder.build())
    ThreadUtils.awaitResult(responseObserver.promise.future, 10.seconds)
  }

  private def extValues(exts: Seq[protobuf.Any]): Seq[String] =
    exts.map(_.unpack(classOf[StringValue]).getValue)

  test("Interrupt returns session info and no extensions when no plugin is registered") {
    SparkConnectPluginRegistry.setInterruptPluginsForTesting(Seq.empty)
    val sessionHolder = SparkConnectTestUtils.createDummySessionHolder(spark)

    val reqExt = protobuf.Any.pack(StringValue.of("ignored"))
    val response =
      sendInterruptAllRequest(sessionHolder.sessionId, sessionHolder.userId, Seq(reqExt))

    assert(response.getSessionId == sessionHolder.sessionId)
    assert(response.getServerSideSessionId.nonEmpty)
    assert(response.getExtensionsList.isEmpty)
  }

  test("Interrupt still cancels running operations when no plugin is registered") {
    SparkConnectPluginRegistry.setInterruptPluginsForTesting(Seq.empty)
    val sessionHolder = SparkConnectTestUtils.createDummySessionHolder(spark)
    val command = proto.Command.newBuilder().build()
    val executeHolder1 = SparkConnectTestUtils.createDummyExecuteHolder(sessionHolder, command)
    val executeHolder2 = SparkConnectTestUtils.createDummyExecuteHolder(sessionHolder, command)

    val response = sendInterruptAllRequest(sessionHolder.sessionId, sessionHolder.userId)

    assert(response.getInterruptedIdsList.asScala.toSet ==
      Set(executeHolder1.operationId, executeHolder2.operationId))
    assert(response.getExtensionsList.isEmpty)
  }

  test("Interrupt runs plugins before dispatch and attaches their response extensions") {
    val prepared = new AtomicBoolean(false)
    SparkConnectPluginRegistry.setInterruptPluginsForTesting(
      Seq(new EchoInterruptPlugin(), new RecordingInterruptPlugin(prepared)))
    val sessionHolder = SparkConnectTestUtils.createDummySessionHolder(spark)
    val command = proto.Command.newBuilder().build()
    val executeHolder = SparkConnectTestUtils.createDummyExecuteHolder(sessionHolder, command)

    val reqExt = protobuf.Any.pack(StringValue.of("req-data"))
    val response =
      sendInterruptAllRequest(sessionHolder.sessionId, sessionHolder.userId, Seq(reqExt))

    // The plugin ran, and the operation is interrupted and its response extension attached.
    assert(prepared.get())
    assert(response.getInterruptedIdsList.asScala.toSet == Set(executeHolder.operationId))
    assert(extValues(response.getExtensionsList.asScala.toSeq) == Seq("request-echo:req-data"))
  }

  test("Interrupt aggregates extensions from multiple plugins, skipping empty ones") {
    SparkConnectPluginRegistry.setInterruptPluginsForTesting(
      Seq(new EchoInterruptPlugin(), new NoOpInterruptPlugin(), new SecondEchoInterruptPlugin()))
    val sessionHolder = SparkConnectTestUtils.createDummySessionHolder(spark)

    val reqExt = protobuf.Any.pack(StringValue.of("hello"))
    val response =
      sendInterruptAllRequest(sessionHolder.sessionId, sessionHolder.userId, Seq(reqExt))

    assert(extValues(response.getExtensionsList.asScala.toSeq).toSet ==
      Set("request-echo:hello", "second-request:hello"))
  }

  test("Interrupt filters a null extension element returned by a plugin") {
    SparkConnectPluginRegistry.setInterruptPluginsForTesting(
      Seq(new NullElementInterruptPlugin(), new EchoInterruptPlugin()))
    val sessionHolder = SparkConnectTestUtils.createDummySessionHolder(spark)

    val reqExt = protobuf.Any.pack(StringValue.of("data"))
    val response =
      sendInterruptAllRequest(sessionHolder.sessionId, sessionHolder.userId, Seq(reqExt))

    assert(extValues(response.getExtensionsList.asScala.toSeq) == Seq("request-echo:data"))
  }

  test("Interrupt by OPERATION_ID cancels the operation and runs plugins") {
    val prepared = new AtomicBoolean(false)
    SparkConnectPluginRegistry.setInterruptPluginsForTesting(
      Seq(new RecordingInterruptPlugin(prepared)))
    val sessionHolder = SparkConnectTestUtils.createDummySessionHolder(spark)
    val command = proto.Command.newBuilder().build()
    val executeHolder = SparkConnectTestUtils.createDummyExecuteHolder(sessionHolder, command)

    val reqExt = protobuf.Any.pack(StringValue.of("op-req"))
    val response = sendInterruptRequest(
      sessionHolder.sessionId,
      sessionHolder.userId,
      { builder =>
        builder
          .setInterruptType(proto.InterruptRequest.InterruptType.INTERRUPT_TYPE_OPERATION_ID)
          .setOperationId(executeHolder.operationId)
          .addExtensions(reqExt)
      })

    assert(prepared.get())
    assert(response.getInterruptedIdsList.asScala.toSet == Set(executeHolder.operationId))
  }

  test("Interrupt does not invoke plugins when the request carries no extensions") {
    val prepared = new AtomicBoolean(false)
    // Would throw on a "boom" extension, but with no extensions it must not run at all.
    SparkConnectPluginRegistry.setInterruptPluginsForTesting(
      Seq(new RecordingInterruptPlugin(prepared)))
    val sessionHolder = SparkConnectTestUtils.createDummySessionHolder(spark)
    val command = proto.Command.newBuilder().build()
    val executeHolder = SparkConnectTestUtils.createDummyExecuteHolder(sessionHolder, command)

    val response = sendInterruptAllRequest(sessionHolder.sessionId, sessionHolder.userId)

    assert(!prepared.get())
    assert(response.getInterruptedIdsList.asScala.toSet == Set(executeHolder.operationId))
    assert(response.getExtensionsList.isEmpty)
  }

  test("Interrupt by TAG cancels only tagged operations") {
    SparkConnectPluginRegistry.setInterruptPluginsForTesting(Seq.empty)
    val sessionHolder = SparkConnectTestUtils.createDummySessionHolder(spark)
    val command = proto.Command.newBuilder().build()
    val tagged =
      SparkConnectTestUtils.createDummyExecuteHolder(sessionHolder, command, Seq("my-tag"))
    val untagged = SparkConnectTestUtils.createDummyExecuteHolder(sessionHolder, command)

    val response = sendInterruptRequest(
      sessionHolder.sessionId,
      sessionHolder.userId,
      _.setInterruptType(proto.InterruptRequest.InterruptType.INTERRUPT_TYPE_TAG)
        .setOperationTag("my-tag"))

    assert(response.getInterruptedIdsList.asScala.toSet == Set(tagged.operationId))
    assert(sessionHolder.listActiveOperationIds().contains(untagged.operationId))
  }

  test("Interrupt errors on a malformed request without invoking plugins") {
    val prepared = new AtomicBoolean(false)
    SparkConnectPluginRegistry.setInterruptPluginsForTesting(
      Seq(new RecordingInterruptPlugin(prepared)))
    val sessionHolder = SparkConnectTestUtils.createDummySessionHolder(spark)

    // INTERRUPT_TYPE_TAG with no operation_tag fails validation before plugins run.
    val e = intercept[Exception] {
      sendInterruptRequest(
        sessionHolder.sessionId,
        sessionHolder.userId,
        _.setInterruptType(proto.InterruptRequest.InterruptType.INTERRUPT_TYPE_TAG))
    }
    assert(e.getMessage.contains("INVALID_PARAMETER_VALUE.INTERRUPT_TYPE_TAG_REQUIRES_TAG"))
    assert(!prepared.get())
  }

  test("Interrupt aborts dispatch when a recognized extension fails to prepare") {
    val prepared = new AtomicBoolean(false)
    SparkConnectPluginRegistry.setInterruptPluginsForTesting(
      Seq(new RecordingInterruptPlugin(prepared)))
    val sessionHolder = SparkConnectTestUtils.createDummySessionHolder(spark)
    val command = proto.Command.newBuilder().build()
    val executeHolder = SparkConnectTestUtils.createDummyExecuteHolder(sessionHolder, command)

    val badExt = protobuf.Any.pack(StringValue.of("boom"))
    val e = intercept[Exception] {
      sendInterruptAllRequest(sessionHolder.sessionId, sessionHolder.userId, Seq(badExt))
    }

    // A recognized-but-failed extension aborts: the operation stays active, never interrupted.
    assert(e.getMessage.contains("malformed recognized extension"))
    assert(sessionHolder.listActiveOperationIds().contains(executeHolder.operationId))
    assert(sessionHolder.getInactiveOperationInfo(executeHolder.operationId).isEmpty)
  }
}

private class InterruptResponseObserver extends StreamObserver[proto.InterruptResponse] {
  val promise: Promise[InterruptResponse] = Promise()
  override def onNext(value: proto.InterruptResponse): Unit = promise.success(value)
  override def onError(t: Throwable): Unit = promise.failure(t)
  override def onCompleted(): Unit = {}
}
