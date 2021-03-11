package org.apache.flink.runtime.state.heap;

import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.internal.InternalAsyncIntegerValueState;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

public class HeapAsyncIntegerValueState<K, N>
	extends AbstractHeapState<K, N, Long>
	implements InternalAsyncIntegerValueState<K, N> {

	public HeapAsyncIntegerValueState(
		StateTable<K, N, Long> stateTable,
		TypeSerializer<K> keySerializer,
		TypeSerializer<Long> valueSerializer,
		TypeSerializer<N> namespaceSerializer,
		Long defaultValue) {
		super(stateTable, keySerializer, valueSerializer, namespaceSerializer, defaultValue);
	}

	@Override
	public CompletableFuture<Long> value() throws IOException {
		final Long result = stateTable.get(currentNamespace);

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
	public CompletableFuture<String> update(Long value) throws IOException {
		if (value == null) {
			clear();
			return CompletableFuture.completedFuture("OK");
		}

		stateTable.put(currentNamespace, value);
		return CompletableFuture.completedFuture("OK");
	}

	@Override
	public CompletableFuture<Long> incr() {
		return null;
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
	public TypeSerializer<Long> getValueSerializer() {
		return valueSerializer;
	}

	@SuppressWarnings("unchecked")
	static <K, N, SV, S extends State, IS extends S> IS create(
		StateDescriptor<S, SV> stateDesc,
		StateTable<K, N, SV> stateTable,
		TypeSerializer<K> keySerializer) {
		return (IS) new HeapAsyncIntegerValueState<>(
			(StateTable<K, N, Long>)stateTable,
			keySerializer,
			(TypeSerializer<Long>)stateTable.getStateSerializer(),
			stateTable.getNamespaceSerializer(),
			(Long)stateDesc.getDefaultValue());
	}
}

