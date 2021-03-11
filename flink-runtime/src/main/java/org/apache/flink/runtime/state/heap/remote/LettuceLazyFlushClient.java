package org.apache.flink.runtime.state.heap.remote;

import io.lettuce.core.LettuceFutures;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.resource.ClientResources;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class LettuceLazyFlushClient implements RemoteKVSyncClient, RemoteKVAsyncClient {

	private static final Logger LOG = LoggerFactory.getLogger(LettuceClient.class);

	private RedisClient db;

	private StatefulRedisConnection<byte[], byte[]> connection;

	private RedisAsyncCommands<byte[], byte[]> commands;

	private ArrayList<RedisFuture<?>> cachedFutures = new ArrayList<>();

	private ByteArrayCodec codec;

	private Timer timer;

	public int interval = 500;

	@Override
	public byte[] get(byte[] key) {
		try {
			CompletableFuture<byte[]> future = commands.get(key).toCompletableFuture();
			commands.flushCommands();
			return future.get();
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (ExecutionException e) {
			e.printStackTrace();
		}
		return null;
	}

	@Nullable
	@Override
	public Object set(byte[] key, byte[] value) {
		try {
			CompletableFuture<String> future = commands.set(key, value).toCompletableFuture();
			commands.flushCommands();
			return future.get();
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (ExecutionException e) {
			e.printStackTrace();
		}
		return null;
	}

	@Override
	public Long incr(byte[] key) {
		CompletableFuture<Long> future = commands.incr(key).toCompletableFuture();
		commands.flushCommands();
		try {
			return future.get();
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (ExecutionException e) {
			e.printStackTrace();
		}
		return null;
	}

	@Override
	public byte[] hget(byte[] key, byte[] field) {
		try {
			return commands.hget(key, field).toCompletableFuture().get();
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (ExecutionException e) {
			e.printStackTrace();
		}
		return null;
	}

	@Override
	public Map<byte[], byte[]> hgetAll(byte[] key) {
		try {
			return commands.hgetall(key).toCompletableFuture().get();
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (ExecutionException e) {
			e.printStackTrace();
		}
		return null;
	}

	@Nullable
	@Override
	public Object hset(byte[] key, byte[] field, byte[] value) {
		try {
			return commands.hset(key, field, value).toCompletableFuture().get();
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (ExecutionException e) {
			e.printStackTrace();
		}
		return null;
	}

	@Override
	public Collection<byte[]> hkeys(byte[] key) {
		try {
			return commands.hkeys(key).toCompletableFuture().get();
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (ExecutionException e) {
			e.printStackTrace();
		}
		return null;
	}

	@Nullable
	@Override
	public Object hdel(byte[] key, byte[]... fields) {
		try {
			return commands.hdel(key, fields).toCompletableFuture().get();
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (ExecutionException e) {
			e.printStackTrace();
		}
		return null;
	}

	@Override
	public Boolean hexists(byte[] key, byte[] field) {
		try {
			return commands.hexists(key, field).toCompletableFuture().get();
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (ExecutionException e) {
			e.printStackTrace();
		}
		return null;
	}

	@Nullable
	@Override
	public Object del(byte[] key) {
		try {
			return commands.del(key).toCompletableFuture().get();
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (ExecutionException e) {
			e.printStackTrace();
		}
		return null;
	}

	@Override
	public Long dbSize() {
		try {
			return commands.dbsize().toCompletableFuture().get();
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (ExecutionException e) {
			e.printStackTrace();
		}
		return null;
	}

	@Override
	public Collection<String> keys(String predicate) {
		try {
			Charset charset = Charset.forName("UTF-8");
			return commands.keys(codec.decodeKey(charset.encode(predicate))).toCompletableFuture().get().stream().map(x-> charset.decode(codec.encodeKey(x)).toString()).collect(
				Collectors.toSet());
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (ExecutionException e) {
			e.printStackTrace();
		}
		return null;
	}

	@Override
	public Long rpush(byte[] key, byte[]... strings) {
		try {
			return commands.rpush(key, strings).toCompletableFuture().get();
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (ExecutionException e) {
			e.printStackTrace();
		}
		return null;
	}

	@Override
	public Long lpush(byte[] key, byte[]... strings) {
		try {
			return commands.lpush(key, strings).toCompletableFuture().get();
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (ExecutionException e) {
			e.printStackTrace();
		}
		return null;
	}

	@Override
	public void pipelineHSet(byte[] key, byte[] field, byte[] value) {
		commands.setAutoFlushCommands(false);
		cachedFutures.add(commands.hset(key, field, value));
	}

	@Override
	public void pipelineHDel(byte[] key, byte[] field) {
		commands.setAutoFlushCommands(false);
		cachedFutures.add(commands.hdel(key, field));
	}

	@Nullable
	@Override
	public Object getAndSet(byte[] key, byte[] value) {
		return null;
	}

	@Override
	public void pipelineSync() {
		commands.flushCommands();
		LettuceFutures.awaitAll(100000, TimeUnit.SECONDS,
			cachedFutures.toArray(new RedisFuture[cachedFutures.size()]));
		commands.setAutoFlushCommands(true);
	}

	@Override
	public void pipelineClose() { }

	@Override
	public void openDB(String host) {
		RedisURI redisUri = RedisURI.Builder.redis(host, 6379).withPassword("authentication").build();
		db = RedisClient.create(redisUri);
		ClientResources resources = db.getResources();
		codec = new ByteArrayCodec();
		connection = db.connect(codec);
		commands = connection.async();
		commands.setAutoFlushCommands(false);
		Timer timer = new Timer();
		timer.schedule(new TimerTask() {
			@Override
			public void run() {
				System.out.println("commands flush " + Thread.currentThread().getName() +
					" host " + host +
					" current queue content " +
					(resources==null?"null":resources.getCommandBuffer().stream().map(x->x.toString()).collect(Collectors.joining(","))));
				commands.flushCommands();
			}
		}, interval, interval);
		LOG.info("Connection from Lettuce Lazy Flush Client to Redis Cluster {} successful with interval {}."
			, host, interval);
	}

	@Override
	public void closeDB() {
		connection.close();
		timer.cancel();
	}

	@Override
	public CompletableFuture<byte[]> getAsync(byte[] key) {
		return commands.get(key).toCompletableFuture();
	}

	@Nullable
	@Override
	public CompletableFuture<String> setAsync(byte[] key, byte[] value) {
		return commands.set(key, value).toCompletableFuture();
	}

	@Override
	public CompletableFuture<Long> incrAsync(byte[] key) {
		return commands.incr(key).toCompletableFuture();
	}

	@Override
	public CompletableFuture<byte[]> hgetAsync(byte[] key, byte[] field) {
		return commands.hget(key, field).toCompletableFuture();
	}

	@Override
	public CompletableFuture<Map<byte[], byte[]>> hgetAllAsync(byte[] key) {
		return commands.hgetall(key).toCompletableFuture();
	}

	@Nullable
	@Override
	public CompletableFuture<Boolean> hsetAsync(byte[] key, byte[] field, byte[] value) {
		return commands.hset(key, field, value).toCompletableFuture();
	}

	@Override
	public CompletableFuture<Collection<byte[]>> hkeysAsync(byte[] key) {;
		return commands.hkeys(key).toCompletableFuture().thenApply(x->x);
	}

	@Nullable
	@Override
	public CompletableFuture<Long> hdelAsync(byte[] key, byte[]... fields) {
		return commands.hdel(key, fields).toCompletableFuture();
	}

	@Override
	public CompletableFuture<Boolean> hexistsAsync(byte[] key, byte[] field) {
		return commands.hexists(key, field).toCompletableFuture();
	}

	@Nullable
	@Override
	public CompletableFuture<Long> delAsync(byte[] key) {
		return commands.del(key).toCompletableFuture();
	}

	@Override
	public CompletableFuture<Long> rpushAsync(byte[] key, byte[]... strings) {
		return commands.rpush(key, strings).toCompletableFuture();
	}

	@Override
	public CompletableFuture<Long> lpushAsync(byte[] key, byte[]... strings) {
		return commands.lpush(key, strings).toCompletableFuture();
	}

	@Nullable
	@Override
	public CompletableFuture<String> getAndSetAsync(byte[] key, byte[] value) {
		return null;
	}
}
