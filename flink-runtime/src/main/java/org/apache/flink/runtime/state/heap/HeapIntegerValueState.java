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

package org.apache.flink.runtime.state.heap;

import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.internal.InternalIntegerValueState;

import java.io.IOException;

/**
 * Heap-backed partitioned {@link ValueState} that is snapshotted into files.
 *
 * @param <K> The type of the key.
 * @param <N> The type of the namespace.
 */
class HeapIntegerValueState<K, N>
	extends AbstractHeapState<K, N, Long>
	implements InternalIntegerValueState<K, N> {

	/**
	 * Creates a new key/value state for the given hash map of key/value pairs.
	 *
	 * @param stateTable The state table for which this state is associated to.
	 * @param keySerializer The serializer for the keys.
	 * @param valueSerializer The serializer for the state.
	 * @param namespaceSerializer The serializer for the namespace.
	 * @param defaultValue The default value for the state.
	 */
	private HeapIntegerValueState(
		StateTable<K, N, Long> stateTable,
		TypeSerializer<K> keySerializer,
		TypeSerializer<Long> valueSerializer,
		TypeSerializer<N> namespaceSerializer,
		Long defaultValue) {
		super(stateTable, keySerializer, valueSerializer, namespaceSerializer, defaultValue);
	}

	@Override
	public TypeSerializer<K> getKeySerializer() {
		return keySerializer;
	}

	@Override
	public TypeSerializer<N> getNamespaceSerializer() {
		return namespaceSerializer;
	}

	@Override
	public TypeSerializer<Long> getValueSerializer() {
		return valueSerializer;
	}

	@Override
	public Long value() {
		final Long result = stateTable.get(currentNamespace);

		if (result == null) {
			return getDefaultValue();
		}

		return result;
	}

	/**
	 * Updates the operator state accessible by {@link #value()} to the given
	 * value. The next time {@link #value()} is called (for the same state
	 * partition) the returned state will represent the updated value. When a
	 * partitioned state is updated with null, the state for the current key
	 * will be removed and the default value is returned on the next access.
	 *
	 * @param value The new value for the state.
	 * @throws IOException Thrown if the system cannot access the state.
	 */
	@Override
	public void update(Long value) throws IOException {
		if (value == null) {
			clear();
			return;
		}

		stateTable.put(currentNamespace, value);
	}

	@Override
	public Long incr() {
		Long result = stateTable.get(currentNamespace);

		if (result == null) {
			result = getDefaultValue();
		}
		result++;
		stateTable.put(currentNamespace, result);
		return result;
	}

	@SuppressWarnings("unchecked")
	static <K, N, SV, S extends State, IS extends S> IS create(
		StateDescriptor<S, SV> stateDesc,
		StateTable<K, N, SV> stateTable,
		TypeSerializer<K> keySerializer) {
		return (IS) new HeapIntegerValueState<K, N>(
			(StateTable<K, N, Long>)stateTable,
			keySerializer,
			(TypeSerializer<Long>)stateTable.getStateSerializer(),
			stateTable.getNamespaceSerializer(),
			(Long)stateDesc.getDefaultValue());
	}
}
