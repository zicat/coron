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

package org.zicat.coron.core;

import java.util.ArrayList;

/**
 * NotNullList.
 *
 * @param <T>
 */
public class ResultNodeList<T> extends ArrayList<ResultNode<T>> {

    public ResultNodeList() {
        super();
    }

    public ResultNodeList(int size) {
        super(size);
    }

    public ResultNodeList(ResultNode<T> e) {
        super();
        add(e);
    }

    @Override
    public boolean add(ResultNode<T> e) {
        if (e == null || e.isEmpty()) {
            return false;
        }
        return super.add(e);
    }
}
