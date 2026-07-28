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

import scala.jdk.CollectionConverters._
import scala.jdk.OptionConverters._

import io.grpc.stub.StreamObserver

import org.apache.spark.SparkSQLException
import org.apache.spark.connect.proto
import org.apache.spark.internal.Logging
import org.apache.spark.sql.connect.plugin.SparkConnectPluginRegistry

class SparkConnectInterruptHandler(responseObserver: StreamObserver[proto.InterruptResponse])
    extends Logging {

  def handle(v: proto.InterruptRequest): Unit = {
    val previousSessionId = v.hasClientObservedServerSideSessionId match {
      case true => Some(v.getClientObservedServerSideSessionId)
      case false => None
    }
    val sessionHolder =
      SparkConnectService
        .getOrCreateIsolatedSession(v.getUserContext.getUserId, v.getSessionId, previousSessionId)

    validateRequest(v)

    val responseBuilder = proto.InterruptResponse
      .newBuilder()
      .setSessionId(v.getSessionId)
      .setServerSideSessionId(sessionHolder.serverSessionId)

    // Run plugins before dispatching, so a plugin can prepare state the cancellation reads. A
    // plugin that recognizes its extension but fails throws, aborting the interrupt. Only invoked
    // when the request carries extensions, so a plain interrupt never runs plugin code.
    if (!v.getExtensionsList.isEmpty) {
      runInterruptPlugins(sessionHolder, v).foreach(responseBuilder.addExtensions)
    }

    val interruptedIds = v.getInterruptType match {
      case proto.InterruptRequest.InterruptType.INTERRUPT_TYPE_ALL =>
        sessionHolder.interruptAll()
      case proto.InterruptRequest.InterruptType.INTERRUPT_TYPE_TAG =>
        sessionHolder.interruptTag(v.getOperationTag)
      case proto.InterruptRequest.InterruptType.INTERRUPT_TYPE_OPERATION_ID =>
        sessionHolder.interruptOperation(v.getOperationId)
      case other =>
        throw new SparkSQLException(
          errorClass = "UNSUPPORTED_FEATURE.INTERRUPT_TYPE",
          messageParameters = Map("interruptType" -> other.toString))
    }
    responseBuilder.addAllInterruptedIds(interruptedIds.asJava)

    responseObserver.onNext(responseBuilder.build())
    responseObserver.onCompleted()
  }

  /** Validate the request up front so a malformed request fails before any plugin runs. */
  private def validateRequest(v: proto.InterruptRequest): Unit = {
    v.getInterruptType match {
      case proto.InterruptRequest.InterruptType.INTERRUPT_TYPE_TAG if !v.hasOperationTag =>
        throw new SparkSQLException(
          errorClass = "INVALID_PARAMETER_VALUE.INTERRUPT_TYPE_TAG_REQUIRES_TAG",
          messageParameters = Map("parameter" -> "operation_tag", "functionName" -> "interrupt"))
      case proto.InterruptRequest.InterruptType.INTERRUPT_TYPE_OPERATION_ID if !v.hasOperationId =>
        throw new SparkSQLException(
          errorClass = "INVALID_PARAMETER_VALUE.INTERRUPT_TYPE_OPERATION_ID_REQUIRES_ID",
          messageParameters = Map("parameter" -> "operation_id", "functionName" -> "interrupt"))
      case _ => // ALL, valid TAG/OPERATION_ID, and unsupported types are handled at dispatch.
    }
  }

  /**
   * Runs every registered [[org.apache.spark.sql.connect.plugin.InterruptPlugin]] before the
   * interrupt is dispatched, collecting the response extensions they return. A plugin that doesn't
   * handle the request returns [[None]]; a plugin that recognizes its extension but fails throws,
   * aborting the interrupt.
   */
  private def runInterruptPlugins(
      sessionHolder: SessionHolder,
      request: proto.InterruptRequest): Seq[com.google.protobuf.Any] = {
    SparkConnectPluginRegistry.interruptRegistry.flatMap { plugin =>
      plugin.prepareInterrupt(sessionHolder, request).toScala match {
        // Filter nulls inside the isolation boundary so a null element can't NPE addExtensions.
        case Some(extensions) => extensions.asScala.iterator.filter(_ != null).toSeq
        case None => Seq.empty
      }
    }
  }
}
