package org.apache.flink.api.common.state;

public interface IntegerValueState extends ValueState<Long> {
	Long incr();
}
