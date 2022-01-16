/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.zicat.coron.parser.util;

import java.util.concurrent.locks.Lock;

/** LockUtils. */
public class LockUtils {

    /**
     * lock function.
     *
     * @param functionHandler functionHandler
     * @param lock lock
     * @param <T> type
     * @return function result.
     */
    public static <T> T lock(FunctionHandler<T> functionHandler, Lock lock) {
        lock.lock();
        try {
            return functionHandler.apply();
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            lock.unlock();
        }
    }

    /**
     * FunctionHandler.
     *
     * @param <T>
     */
    public interface FunctionHandler<T> {

        /**
         * invoke function.
         *
         * @return result
         */
        T apply() throws Exception;
    }
}
