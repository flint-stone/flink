package org.apache.flink.runtime.state.heap.remote;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.codec.ByteArrayCodec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.stream.Collectors;

public class LettuceSyncClient implements RemoteKVSyncClient {

	private static final Logger LOG = LoggerFactory.getLogger(LettuceSyncClient.class);

	private RedisClient db;

	private StatefulRedisConnection<byte[], byte[]> connection;

	private RedisCommands<byte[], byte[]> commands;

	private ArrayList<RedisFuture<?>> cachedFutures = new ArrayList<>();

	private ByteArrayCodec codec;

	public static LettuceSyncClient Client = new LettuceSyncClient();

	public Boolean initialized = false;

	@Override
	public byte[] get(byte[] key) {
		return commands.get(key);
	}

	@Nullable
	@Override
	public Object set(byte[] key, byte[] value) {
		return commands.set(key, value);
	}

	@Override
	public Long incr(byte[] key) {
		return commands.incr(key);
	}

	@Override
	public Object multi() {
		return commands.multi();
	}

	@Override
	public Object exec() {
		return commands.exec();
	}

	@Override
	public byte[] hget(byte[] key, byte[] field) {
		return commands.hget(key, field);
	}

	@Override
	public Map<byte[], byte[]> hgetAll(byte[] key) {
		return commands.hgetall(key);
	}

	@Nullable
	@Override
	public Object hset(byte[] key, byte[] field, byte[] value) {
		return commands.hset(key, field, value);
	}

	@Override
	public Collection<byte[]> hkeys(byte[] key) {
		return commands.hkeys(key);
	}

	@Nullable
	@Override
	public Object hdel(byte[] key, byte[]... fields) {
		return commands.hdel(key, fields);
	}

	@Override
	public Boolean hexists(byte[] key, byte[] field) {
		return commands.hexists(key, field);
	}

	@Nullable
	@Override
	public Object del(byte[] key) {
		return commands.del(key);
	}

	@Override
	public Long dbSize() {
		return commands.dbsize();
	}

	@Override
	public Collection<String> keys(String predicate) {
		Charset charset = Charset.forName("UTF-8");
		return commands.keys(codec.decodeKey(charset.encode(predicate))).stream().map(x-> charset.decode(codec.encodeKey(x)).toString()).collect(
			Collectors.toSet());
	}

	@Override
	public Long rpush(byte[] key, byte[]... strings) {
		return commands.rpush(key, strings);
	}

	@Override
	public Long lpush(byte[] key, byte[]... strings) {
		return commands.lpush(key, strings);
	}

	@Override
	public void pipelineHSet(byte[] key, byte[] field, byte[] value) {
		try {
			throw new Exception("pipelineHSet Not Implemented.");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public void pipelineHDel(byte[] key, byte[] field) {
		try {
			throw new Exception("pipelineHDel Not Implemented.");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Nullable
	@Override
	public Object getAndSet(byte[] key, byte[] value) {
		return null;
	}

	@Override
	public void pipelineSync() {
		try {
			throw new Exception("pipelineSync Not Implemented.");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	@Override
	public void pipelineClose() { }

	@Override
	public void openDB(String host) {
		synchronized (initialized){
			if(!initialized){
				RedisURI redisUri = RedisURI.Builder.redis(host, 6379).withPassword("authentication").build();
				db = RedisClient.create(redisUri);
				codec = new ByteArrayCodec();
				connection = db.connect(codec);
				commands = connection.sync();
				initialized = true;
				LOG.info("Connection from Lettuce Sync Client to Redis Cluster {} successful.", host);
			}
		}

	}

	public LettuceSyncClient(){
		LOG.info("Initialize LettuceSyncClient once at tid {}", Thread.currentThread().getName());
	}

	@Override
	public void closeDB() {
		connection.close();
		initialized = false;
	}

}
