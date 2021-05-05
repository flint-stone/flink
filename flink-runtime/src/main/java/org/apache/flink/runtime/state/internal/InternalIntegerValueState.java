package org.apache.flink.runtime.state.internal;

import org.apache.flink.api.common.state.IntegerValueState;

public interface InternalIntegerValueState<K, N> extends InternalKvState<K, N, Long>, IntegerValueState {}

