package org.apache.flink.runtime.state.heap.remote;

import javax.annotation.Nullable;

import java.util.Collection;
import java.util.List;
import java.util.Map;

public interface RemoteKVSyncClient extends RemoteKVClient {

	byte[] get(byte[] key);

	@Nullable
	Object set(byte[] key, byte[] value);

	Long incr(byte[] key);

	Object multi();

	Object exec();

	byte[] hget(byte[] key, byte[] field);

	Map<byte[], byte[]> hgetAll(byte[] key);

	@Nullable
	Object hset(byte[] key, byte[] field, byte[] value);

	Collection<byte[]> hkeys(byte[] key);

	@Nullable
	Object hdel(byte[] key, byte[]... fields);

	Boolean hexists(byte[] key, byte[] field);

	@Nullable
	Object del(byte[] key);

	Long rpush(byte[] key, byte[]... strings);

	Long lpush(byte[] key, byte[]... strings);

	List<byte[]> lrange(byte[]key, int lIndex, int rIndex);

	void pipelineHSet(byte[] key, byte[] field, byte[] value);

	void pipelineHDel(byte[] key, byte[] field);

	@Nullable
	Object getAndSet(byte[] key, byte[]value);
}
