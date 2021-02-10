/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.examples.async;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.AsyncValueState;
import org.apache.flink.api.common.state.AsyncValueStateDescriptor;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.query.UnknownKvStateLocation;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.async.AsyncFunction;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.ExecutorUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Example to illustrates how to use {@link AsyncFunction}.
 */
public class AsyncIOExample {

	private static final Logger LOG = LoggerFactory.getLogger(AsyncIOExample.class);

	private static final String EXACTLY_ONCE_MODE = "exactly_once";
	private static final String EVENT_TIME = "EventTime";
	private static final String INGESTION_TIME = "IngestionTime";
	private static final String ORDERED = "ordered";

	/**
	 * A checkpointed source.
	 */
	private static class SimpleSource implements SourceFunction<Tuple2<Integer, Integer>>, CheckpointedFunction {
		private static final long serialVersionUID = 1L;

		private volatile boolean isRunning = true;
		private int counter = 0;
		private int start = 0;

		private ListState<Integer> state;

		@Override
		public void initializeState(FunctionInitializationContext context) throws Exception {
			state = context.getOperatorStateStore().getListState(new ListStateDescriptor<>(
					"state",
					IntSerializer.INSTANCE));

			// restore any state that we might already have to our fields, initialize state
			// is also called in case of restore.
			for (Integer i : state.get()) {
				start = i;
			}
		}

		@Override
		public void snapshotState(FunctionSnapshotContext context) throws Exception {
			state.clear();
			state.add(start);
		}

		public SimpleSource(int maxNum) {
			this.counter = maxNum;
		}

		@Override
		public void run(SourceContext<Tuple2<Integer, Integer>> ctx) throws Exception {
			while ((start < counter || counter == -1) && isRunning) {
				synchronized (ctx.getCheckpointLock()) {
					ctx.collect(new Tuple2<Integer, Integer>(start, start));
					++start;

					// loop back to 0
					if (start == Integer.MAX_VALUE) {
						start = 0;
					}
				}
				Thread.sleep(10L);
			}
		}

		@Override
		public void cancel() {
			isRunning = false;
		}
	}


	/**
	 * An example of {@link AsyncFunction} using a thread pool and executing working threads
	 * to simulate multiple async operations.
	 *
	 * <p>For the real use case in production environment, the thread pool may stay in the
	 * async client.
	 */
	private static class SampleAsyncFunction extends RichAsyncFunction<Tuple2<Integer, Integer>, String> {
		private static final long serialVersionUID = 2098635244857937717L;

		private transient ExecutorService executorService;

		/**
		 * The result of multiplying sleepFactor with a random float is used to pause
		 * the working thread in the thread pool, simulating a time consuming async operation.
		 */
		private final long sleepFactor;

		/**
		 * The ratio to generate an exception to simulate an async error. For example, the error
		 * may be a TimeoutException while visiting HBase.
		 */
		private final float failRatio;

		private final long shutdownWaitTS;

		private final ReentrantLock lock;

		private AsyncValueStateDescriptor<Tuple2<Long, Long>> descriptor;
		//private ValueStateDescriptor<Tuple2<Long, Long>> descriptor;

		SampleAsyncFunction(long sleepFactor, float failRatio, long shutdownWaitTS) {
			this.sleepFactor = sleepFactor;
			this.failRatio = failRatio;
			this.shutdownWaitTS = shutdownWaitTS;
			this.lock = new ReentrantLock();
		}

		@Override
		public void open(Configuration parameters) throws Exception {
			super.open(parameters);
			executorService = Executors.newFixedThreadPool(10);
			descriptor =
				new AsyncValueStateDescriptor<>(
					"average", // the state name
					TypeInformation.of(new TypeHint<Tuple2<Long, Long>>() {}), // type information
					Tuple2.of(0L, 0L)); // def
		}

		@Override
		public void close() throws Exception {
			super.close();
			ExecutorUtils.gracefulShutdown(shutdownWaitTS, TimeUnit.MILLISECONDS, executorService);
		}

		@Override
		public void asyncInvoke(final Tuple2<Integer, Integer> input, final ResultFuture<String> resultFuture) {
			System.out.println("asyncInvoke handle request " + Thread.currentThread().getName());
			RichAsyncFunctionRuntimeContext runtimeContext = (RichAsyncFunctionRuntimeContext)getRuntimeContext();
			executorService.submit(() -> {
				// wait for while to simulate async operation here
				long sleep = (long) (ThreadLocalRandom.current().nextFloat() * sleepFactor);
				System.out.println("asyncInvoke handle request " + Thread.currentThread().getName() + " sleep " + sleep);
				try {
					Thread.sleep(sleep);

					if (ThreadLocalRandom.current().nextFloat() < failRatio) {
						AsyncValueState<Tuple2<Long, Long>> state = runtimeContext.getAsyncState(descriptor);
						state.value()
							.thenApply(tuple -> {
								try {
									return state.update(new Tuple2<>(tuple.f0 + input.f0, tuple.f1 + input.f1));
								} catch (IOException e) {
									e.printStackTrace();
								}
								return CompletableFuture.completedFuture(null);
							})
							.thenRun(()->resultFuture.completeExceptionally(new Exception("wahahahaha...")));
					} else {
//						runtimeContext.getAsyncState(descriptor).thenRun(()->resultFuture.complete(
//							Collections.singletonList("key-" + (input % 10))));
						System.out.println("Spawn state handle before request " + Thread.currentThread().getName());
						AsyncValueState<Tuple2<Long, Long>> state = runtimeContext.getAsyncState(descriptor);
						System.out.println("Spawn state handle after request " + Thread.currentThread().getName());
						lock.lock();
						try {
							state.value()
								.thenApply(tuple -> {
									Tuple2<Long, Long> t = new Tuple2<>(
										tuple.f0 + input.f0,
										tuple.f1 + input.f1);
									try {
										System.out.println(
											"Receive tuple " + tuple + " thread " + Thread
												.currentThread()
												.getName());
										state.update(t).complete(null);
									} catch (IOException e) {
										e.printStackTrace();
									}
									return t;//CompletableFuture.completedFuture(t);
								})
								.thenAccept(tuple -> {
									System.out.println("Update tuple " + tuple + " thread " + Thread
										.currentThread()
										.getName());
									resultFuture.complete(Collections.singletonList(
										"key-" + input.f0 + "-" + input.f1));
								});
						}
						finally{
							lock.unlock();
						}

//							.thenAccept(tuple -> {
//								System.out.println("Receive tuple " + tuple + " thread " + Thread.currentThread().getName());
//								resultFuture.complete(
//									Collections.singletonList("key-" + tuple.f0 + "-" + input.f1));
//							});
					}



					/*
					if (ThreadLocalRandom.current().nextFloat() < failRatio) {
						ValueState<Tuple2<Long, Long>> state = runtimeContext.getState(descriptor);
						state.update(new Tuple2<>(state.value().f0 + input.f0, state.value().f1 + input.f1));

						resultFuture.completeExceptionally(new Exception("wahahahaha..."));
					} else {
//						runtimeContext.getAsyncState(descriptor).thenRun(()->resultFuture.complete(
//							Collections.singletonList("key-" + (input % 10))));
						System.out.println("AsyncIOExample: Spawn state handle before request " + Thread.currentThread().getName() + " input " + input);
						ValueState<Tuple2<Long, Long>> state = runtimeContext.getState(descriptor);
						lock.lock();
						Tuple2<Long, Long> tuple;
						try{
							tuple = state.value();
							System.out.println("AsyncIOExample: Spawn state handle after request " + Thread.currentThread().getName() + " input " + input + " tuple" + tuple);
						}
						finally {
							lock.unlock();
						}

						lock.lock();
						try{
							Tuple2<Long, Long> tnew = new Tuple2<>(tuple.f0 + input.f0, tuple.f1 + input.f1);
							System.out.println("AsyncIOExample: Update tuple " + tnew + " thread " + Thread.currentThread().getName() + " input " + input);
						}
						finally {
							lock.unlock();
						}


						resultFuture.complete(Collections.singletonList("key-" + input.f0+ "-" + input.f1));

//							.thenAccept(tuple -> {
//								System.out.println("Receive tuple " + tuple + " thread " + Thread.currentThread().getName());
//								resultFuture.complete(
//									Collections.singletonList("key-" + tuple.f0 + "-" + input.f1));
//							});
					}
					*/

				} catch (InterruptedException | IOException | UnknownKvStateLocation e) {
					resultFuture.complete(new ArrayList<>(0));
				}
			}
			);


//			System.out.println("asyncInvoke handle request " + Thread.currentThread().getName());
//			RichAsyncFunctionRuntimeContext runtimeContext = (RichAsyncFunctionRuntimeContext)getRuntimeContext();
//			executorService.submit(() -> {
//				// wait for while to simulate async operation here
//				long sleep = (long) (ThreadLocalRandom.current().nextFloat() * sleepFactor);
//				System.out.println("asyncInvoke handle request " + Thread.currentThread().getName() + " sleep " + sleep);
//				try {
//					Thread.sleep(sleep);
//
//					if (ThreadLocalRandom.current().nextFloat() < failRatio) {
//						AsyncValueState<Tuple2<Long, Long>> state = runtimeContext.getAsyncState(descriptor);
//						state.value()
//							.thenApply(tuple -> {
//							try {
//								return state.update(new Tuple2<>(tuple.f0 + 1, tuple.f1 + 1));
//							} catch (IOException e) {
//								e.printStackTrace();
//							}
//							return CompletableFuture.completedFuture(null);
//						})
//							.thenRun(()->resultFuture.completeExceptionally(new Exception("wahahahaha...")));
//					} else {
////						runtimeContext.getAsyncState(descriptor).thenRun(()->resultFuture.complete(
////							Collections.singletonList("key-" + (input % 10))));
//						System.out.println("Spawn state handle before request " + Thread.currentThread().getName());
//						AsyncValueState<Tuple2<Long, Long>> state = runtimeContext.getAsyncState(descriptor);
//						System.out.println("Spawn state handle after request " + Thread.currentThread().getName());
//						state.value()
//							.thenAccept(tuple -> {
//								System.out.println("Receive tuple " + tuple + " thread " + Thread.currentThread().getName());
//								resultFuture.complete(
//									Collections.singletonList("key-" + tuple.f0));
//							});
//					}
//				} catch (InterruptedException | UnknownKvStateLocation | IOException e) {
//					resultFuture.complete(new ArrayList<>(0));
//				}
//			});
		}
	}

	private static void printUsage() {
		System.out.println("To customize example, use: AsyncIOExample [--fsStatePath <path to fs state>] " +
				"[--checkpointMode <exactly_once or at_least_once>] " +
				"[--maxCount <max number of input from source, -1 for infinite input>] " +
				"[--sleepFactor <interval to sleep for each stream element>] [--failRatio <possibility to throw exception>] " +
				"[--waitMode <ordered or unordered>] [--waitOperatorParallelism <parallelism for async wait operator>] " +
				"[--eventType <EventTime or IngestionTime>] [--shutdownWaitTS <milli sec to wait for thread pool>]" +
				"[--timeout <Timeout for the asynchronous operations>] [--queueSize <number of queue entries>]");
	}

	public static void main(String[] args) throws Exception {

		// obtain execution environment
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		// parse parameters
		final ParameterTool params = ParameterTool.fromArgs(args);

		final String statePath;
		final String cpMode;
		final int maxCount;
		final long sleepFactor;
		final float failRatio;
		final String mode;
		final int taskNum;
		final String timeType;
		final long shutdownWaitTS;
		final long timeout;
		final int queueSize;
		final long checkpointInterval;

		try {
			// check the configuration for the job
			statePath = params.get("fsStatePath", null);
			cpMode = params.get("checkpointMode", "exactly_once");
			maxCount = params.getInt("maxCount", 100000);
			sleepFactor = params.getLong("sleepFactor", 100);
			failRatio = params.getFloat("failRatio", 0.001f);
			mode = params.get("waitMode", "ordered");
			taskNum = params.getInt("waitOperatorParallelism", 1);
			timeType = params.get("eventType", "EventTime");
			shutdownWaitTS = params.getLong("shutdownWaitTS", 20000);
			timeout = params.getLong("timeout", 10000L);
			queueSize = params.getInt("queueSize", 20);
			checkpointInterval = params.getLong("checkpointInterval", 1000L);
		} catch (Exception e) {
			printUsage();

			throw e;
		}

		StringBuilder configStringBuilder = new StringBuilder();

		final String lineSeparator = System.getProperty("line.separator");

		configStringBuilder
			.append("Job configuration").append(lineSeparator)
			.append("FS state path=").append(statePath).append(lineSeparator)
			.append("Checkpoint mode=").append(cpMode).append(lineSeparator)
			.append("Max count of input from source=").append(maxCount).append(lineSeparator)
			.append("Sleep factor=").append(sleepFactor).append(lineSeparator)
			.append("Fail ratio=").append(failRatio).append(lineSeparator)
			.append("Waiting mode=").append(mode).append(lineSeparator)
			.append("Parallelism for async wait operator=").append(taskNum).append(lineSeparator)
			.append("Event type=").append(timeType).append(lineSeparator)
			.append("Shutdown wait timestamp=").append(shutdownWaitTS)
			.append("Queue size=").append(queueSize)
			.append("Checkpoint interval=").append(checkpointInterval);

		LOG.info(configStringBuilder.toString());

		if (statePath != null) {
			// setup state and checkpoint mode
			env.setStateBackend(new FsStateBackend(statePath));
		}

		if (checkpointInterval > 0){
			if (EXACTLY_ONCE_MODE.equals(cpMode)) {
				env.enableCheckpointing(checkpointInterval, CheckpointingMode.EXACTLY_ONCE);
			}
			else {
				env.enableCheckpointing(checkpointInterval, CheckpointingMode.AT_LEAST_ONCE);
			}
		}

		// enable watermark or not
		if (EVENT_TIME.equals(timeType)) {
			env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		}
		else if (INGESTION_TIME.equals(timeType)) {
			env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);
		}

		// create input stream of a single integer
		KeyedStream<Tuple2<Integer, Integer>, String> inputStream = env.addSource(new SimpleSource(maxCount)).keyBy(x->"key-" + x.f0 + "-" + x.f1);

		// create async function, which will "wait" for a while to simulate the process of async i/o
		AsyncFunction<Tuple2<Integer, Integer>, String> function =
				new SampleAsyncFunction(sleepFactor, failRatio, shutdownWaitTS);

		// add async operator to streaming job
		DataStream<String> result;
		if (ORDERED.equals(mode)) {
			result = AsyncDataStream.orderedWait(
				inputStream,
				function,
				timeout,
				TimeUnit.MILLISECONDS,
				queueSize).setParallelism(taskNum);
		}
		else {
			result = AsyncDataStream.unorderedWait(
				inputStream,
				function,
				timeout,
				TimeUnit.MILLISECONDS,
				queueSize).setParallelism(taskNum);
		}

		// add a reduce to get the sum of each keys.
		result.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
			private static final long serialVersionUID = -938116068682344455L;

			@Override
			public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
				out.collect(new Tuple2<>(value, 1));
			}
		}).keyBy(value -> value.f0).sum(1).print();

		// execute the program
		env.execute("Async IO Example");
	}
}



