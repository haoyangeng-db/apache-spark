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

package org.apache.spark.sql.connect.plugin;

import com.google.protobuf.Any;

import java.util.List;
import java.util.Optional;

import org.apache.spark.connect.proto.InterruptRequest;
import org.apache.spark.sql.connect.service.SessionHolder;

/**
 * Plugin interface for extending Interrupt RPC behavior in Spark Connect.
 *
 * <p>Classes implementing this interface must be trivially constructable (have a no-argument
 * constructor) and should not rely on internal state; a single instance is shared across all
 * sessions and invoked concurrently.
 *
 * <p>The plugin runs <b>before</b> any execution is interrupted, so it can register state that the
 * execution's own cancellation callback will read.
 */
public interface InterruptPlugin {

    /**
     * Process the interrupt request before any execution is interrupted.
     *
     * <p>Must be synchronous and limited to local parsing / in-memory preparation. Returns the
     * response extensions to attach to the InterruptResponse, or {@link Optional#empty()} if this
     * plugin does not handle the request. If the plugin recognizes its extension but cannot process
     * it, it must throw: the handler then aborts and interrupts nothing.
     *
     * <p>This runs before the interrupt is dispatched, so returning normally does not mean any
     * execution has been interrupted yet; it only means preparation succeeded.
     *
     * @param sessionHolder the session holder for the current session
     * @param request the full InterruptRequest, including its {@code extensions}
     * @return response extensions to add to the InterruptResponse, or empty if none
     */
    Optional<List<Any>> prepareInterrupt(SessionHolder sessionHolder, InterruptRequest request);
}
