package org.apache.flink.runtime.state.heap;

import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.internal.InternalValueState;
import org.apache.flink.runtime.state.internal.InternalAsyncValueState;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

public class HeapAsyncValueState<K, N, V>
	extends AbstractHeapState<K, N, V>
	implements InternalAsyncValueState<K, N, V> {
	public HeapAsyncValueState(
		StateTable<K, N, V> stateTable,
		TypeSerializer<K> keySerializer,
		TypeSerializer<V> valueSerializer,
		TypeSerializer<N> namespaceSerializer,
		V defaultValue) {
		super(stateTable, keySerializer, valueSerializer, namespaceSerializer, defaultValue);
	}

	@Override
	public CompletableFuture<V> value() throws IOException {
		final V result = stateTable.get(currentNamespace);

		if (result == null) {
			return CompletableFuture.completedFuture(getDefaultValue());
		}

		return CompletableFuture.completedFuture(result);
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
	public CompletableFuture<String> update(V value) throws IOException {
		if (value == null) {
			clear();
			return CompletableFuture.completedFuture("OK");
		}

		stateTable.put(currentNamespace, value);
		return CompletableFuture.completedFuture("OK");
	}

	/**
	 * Returns the {@link TypeSerializer} for the type of key this state is associated to.
	 */
	@Override
	public TypeSerializer<K> getKeySerializer() {
		return keySerializer;
	}

	/**
	 * Returns the {@link TypeSerializer} for the type of namespace this state is associated to.
	 */
	@Override
	public TypeSerializer<N> getNamespaceSerializer() {
		return namespaceSerializer;
	}

	/**
	 * Returns the {@link TypeSerializer} for the type of value this state holds.
	 */
	@Override
	public TypeSerializer<V> getValueSerializer() {
		return valueSerializer;
	}

	@SuppressWarnings("unchecked")
	static <K, N, SV, S extends State, IS extends S> IS create(
		StateDescriptor<S, SV> stateDesc,
		StateTable<K, N, SV> stateTable,
		TypeSerializer<K> keySerializer) {
		return (IS) new HeapAsyncValueState<>(
			stateTable,
			keySerializer,
			stateTable.getStateSerializer(),
			stateTable.getNamespaceSerializer(),
			stateDesc.getDefaultValue());
	}
}

