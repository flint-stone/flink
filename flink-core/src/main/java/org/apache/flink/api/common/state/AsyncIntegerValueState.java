package org.apache.flink.api.common.state;

import java.util.concurrent.CompletableFuture;

public interface AsyncIntegerValueState extends AsyncValueState<Long> {
	CompletableFuture<Long> incr();
}
