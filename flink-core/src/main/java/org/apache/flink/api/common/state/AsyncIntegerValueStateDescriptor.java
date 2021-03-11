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

package org.apache.flink.api.common.state;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;

/**
 * {@link StateDescriptor} for {@link ValueState}. This can be used to create partitioned
 * value state using
 * {@link org.apache.flink.api.common.functions.RuntimeContext#getState(ValueStateDescriptor)}.
 *
 * <p>If you don't use one of the constructors that set a default value the value that you
 * get when reading a {@link ValueState} using {@link ValueState#value()} will be {@code null}.
 *
 */
@PublicEvolving
public class AsyncIntegerValueStateDescriptor extends AsyncValueStateDescriptor<Long> {

	private static final long serialVersionUID = 1L;

	@Deprecated
	public AsyncIntegerValueStateDescriptor(String name, Class<Long> typeClass, Long defaultValue) {
		super(name, typeClass, defaultValue);
	}

	@Deprecated
	public AsyncIntegerValueStateDescriptor(String name, TypeInformation<Long> typeInfo, Long defaultValue) {
		super(name, typeInfo, defaultValue);
	}

	@Deprecated
	public AsyncIntegerValueStateDescriptor(String name, TypeSerializer<Long> typeSerializer, Long defaultValue) {
		super(name, typeSerializer, defaultValue);
	}

	public AsyncIntegerValueStateDescriptor(String name, Class<Long> typeClass) {
		super(name, typeClass, null);
	}

	/**
	 * Creates a new {@code ValueStateDescriptor} with the given name and type.
	 *
	 * @param name The (unique) name for the state.
	 * @param typeInfo The type of the values in the state.
	 */
	public AsyncIntegerValueStateDescriptor(String name, TypeInformation<Long> typeInfo) {
		super(name, typeInfo, null);
	}

	/**
	 * Creates a new {@code ValueStateDescriptor} with the given name and the specific serializer.
	 *
	 * @param name The (unique) name for the state.
	 * @param typeSerializer The type serializer of the values in the state.
	 */
	public AsyncIntegerValueStateDescriptor(String name, TypeSerializer<Long> typeSerializer) {
		super(name, typeSerializer, null);
	}

	@Override
	public Type getType() {
		return Type.VALUE;
	}
}
