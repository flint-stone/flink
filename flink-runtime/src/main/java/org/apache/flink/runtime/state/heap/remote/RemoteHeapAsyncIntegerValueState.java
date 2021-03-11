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
import org.apache.flink.runtime.state.internal.InternalAsyncIntegerValueState;
import org.apache.flink.runtime.state.internal.InternalAsyncValueState;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

/**
 * Heap-backed partitioned {@link ValueState} that is snapshotted into files.
 *
 * @param <K> The type of the key.
 * @param <N> The type of the namespace.
 */
class RemoteHeapAsyncIntegerValueState<K, N>
	extends AbstractRemoteHeapState<K, N, Long>
	implements InternalAsyncIntegerValueState<K, N> {
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
	private RemoteHeapAsyncIntegerValueState(
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
	public CompletableFuture<Long> value() {
		CompletableFuture<Long> ret = null;
		try {
			ret = backend.asyncRemClient.getAsync(serializeCurrentKeyWithGroupAndNamespaceDesc(kvStateInfo.nameBytes)).thenApply(valueBytes->{
					if (valueBytes == null) {
						return getDefaultValue();
					}
					dataInputView.setBuffer(valueBytes);
					Long value = null;
					try {
						value = valueSerializer.deserialize(dataInputView);
					} catch (IOException e) {
						e.printStackTrace();
					}
					LOG.debug(
						"RemoteHeapAsyncValueState retrieve value state {} namespace {} key {} thread {}",
						value,
						currentNamespace,
						backend.getCurrentKey(), Thread.currentThread().getName());
					return value;
				}
			);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return ret;
	}

	@Override
	public CompletableFuture<String> update(Long value) throws IOException {
		if (value == null) {
			clear();
			return CompletableFuture.supplyAsync(()->"");
		}

		try {
			LOG.debug(
				"RemoteHeapAsyncValueState update to value state {} namespace {} key {} thread {}",
				value,
				currentNamespace,
				backend.getCurrentKey(), Thread.currentThread().getName());
			return backend.asyncRemClient.setAsync(
				serializeCurrentKeyWithGroupAndNamespaceDesc(kvStateInfo.nameBytes),
				serializeValue(value));

		} catch (IOException e) {
			e.printStackTrace();
		}
		return CompletableFuture.supplyAsync(()->"");
	}

	@Override
	public CompletableFuture<Long> incr() {
		CompletableFuture<Long> ret = null;
		try {
			ret = backend.asyncRemClient
				.incrAsync(serializeCurrentKeyWithGroupAndNamespaceDesc(kvStateInfo.nameBytes))
				.thenApply(value->{
					if (value == null) {
						return getDefaultValue();
					}
					LOG.debug(
						"RemoteHeapAsyncValueState retrieve value state {} namespace {} key {} thread {}",
						value,
						currentNamespace,
						backend.getCurrentKey(), Thread.currentThread().getName());
					return value;
				}
			);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return ret;
	}

	@SuppressWarnings("unchecked")
	static <K, N, SV, S extends State, IS extends S> IS create(
		StateDescriptor<S, SV> stateDesc,
		RegisteredKeyValueStateBackendMetaInfo<N, SV> metaInfo,
		TypeSerializer<K> keySerializer,
		RemoteHeapKeyedStateBackend backend) {
		RemoteHeapKeyedStateBackend.RemoteHeapKvStateInfo kvState = backend.getRemoteHeapKvStateInfo(
			stateDesc.getName());
		return (IS) new RemoteHeapAsyncIntegerValueState<>(
			keySerializer,
			(TypeSerializer<Long>)metaInfo.getStateSerializer(),
			metaInfo.getNamespaceSerializer(),
			kvState,
			(Long)stateDesc.getDefaultValue(),
			backend);
	}
}
