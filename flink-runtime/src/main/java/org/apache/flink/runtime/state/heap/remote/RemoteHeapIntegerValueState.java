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

package org.apache.flink.runtime.state.heap.remote;

import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.RegisteredKeyValueStateBackendMetaInfo;
import org.apache.flink.runtime.state.internal.InternalIntegerValueState;
import org.apache.flink.util.FlinkRuntimeException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Heap-backed partitioned {@link ValueState} that is snapshotted into files.
 *
 * @param <K> The type of the key.
 * @param <N> The type of the namespace.
 */
class RemoteHeapIntegerValueState<K, N>
	extends AbstractRemoteHeapState<K, N, Long>
	implements InternalIntegerValueState<K, N> {
	private static final Logger LOG = LoggerFactory.getLogger(RemoteHeapValueState.class);


	/**
	 * Creates a new key/value state for the given hash map of key/value pairs.
	 *
	 * @param keySerializer The serializer for the keys.
	 * @param valueSerializer The serializer for the state.
	 * @param namespaceSerializer The serializer for the namespace.
	 * @param kvStateInfo StateInfo containing descriptors
	 * @param defaultValue The default value for the state.
	 * @param backend KeyBackend
	 */
	protected RemoteHeapIntegerValueState(
		TypeSerializer<K> keySerializer,
		TypeSerializer<Long> valueSerializer,
		TypeSerializer<N> namespaceSerializer,
		RemoteHeapKeyedStateBackend.RemoteHeapKvStateInfo kvStateInfo,
		Long defaultValue,
		RemoteHeapKeyedStateBackend backend) {
		super(
			keySerializer,
			valueSerializer,
			namespaceSerializer,
			kvStateInfo,
			defaultValue,
			backend);
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
		try {
			byte[] valueBytes = backend.syncRemClient.get(
				serializeCurrentKeyWithGroupAndNamespaceDesc(kvStateInfo.nameBytes));
			if (valueBytes == null) {
				return getDefaultValue();
			}
			dataInputView.setBuffer(valueBytes);
			Long value = valueSerializer.deserialize(dataInputView);
			LOG.debug(
				"RemoteHeapValueState retrieve value state {} namespace {} key {}",
				value,
				currentNamespace,
				backend.getCurrentKey());
			return value;
		} catch (Exception e) {
			throw new FlinkRuntimeException("Error while retrieving data from remote heap.", e);
		}
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
		try {
			backend.syncRemClient.set(
				serializeCurrentKeyWithGroupAndNamespaceDesc(kvStateInfo.nameBytes),
				serializeValue(value));
			LOG.debug(
				"RemoteHeapValueState update to value state {} namespace {} key {}",
				value,
				currentNamespace,
				backend.getCurrentKey());
		} catch (Exception e) {
			throw new FlinkRuntimeException("Error while adding data to RocksDB", e);
		}
	}

	@Override
	public Long incr() {
		try {
			Long value = backend.syncRemClient.incr(
				serializeCurrentKeyWithGroupAndNamespaceDesc(kvStateInfo.nameBytes));
			if (value == null) {
				return getDefaultValue();
			}
			LOG.debug(
				"RemoteHeapValueState retrieve value state {} namespace {} key {}",
				value,
				currentNamespace,
				backend.getCurrentKey());
			return value;
		} catch (Exception e) {
			throw new FlinkRuntimeException("Error while retrieving data from remote heap.", e);
		}
	}

	@SuppressWarnings("unchecked")
	static <K, N, SV, S extends State, IS extends S> IS create(
		StateDescriptor<S, SV> stateDesc,
		RegisteredKeyValueStateBackendMetaInfo<N, SV> metaInfo,
		TypeSerializer<K> keySerializer,
		RemoteHeapKeyedStateBackend backend) {
		RemoteHeapKeyedStateBackend.RemoteHeapKvStateInfo kvState = backend.getRemoteHeapKvStateInfo(
			stateDesc.getName());
		return (IS) new RemoteHeapIntegerValueState<>(
			keySerializer,
			(TypeSerializer<Long>)metaInfo.getStateSerializer(),
			metaInfo.getNamespaceSerializer(),
			kvState,
			(Long)stateDesc.getDefaultValue(),
			backend);
	}
}
