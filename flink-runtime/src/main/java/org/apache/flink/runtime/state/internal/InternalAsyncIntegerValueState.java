package org.apache.flink.runtime.state.internal;

import org.apache.flink.api.common.state.AsyncIntegerValueState;

public interface InternalAsyncIntegerValueState<K, N> extends InternalKvState<K, N, Long>, AsyncIntegerValueState {}
