/**
 * Copyright 2018 Nikita Koksharov
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.redisson.command;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.redisson.SlotCallback;
import org.redisson.api.RFuture;
import org.redisson.api.RedissonClient;
import org.redisson.api.RedissonReactiveClient;
import org.redisson.api.RedissonRxClient;
import org.redisson.client.RedisClient;
import org.redisson.client.RedisException;
import org.redisson.client.codec.Codec;
import org.redisson.client.protocol.RedisCommand;
import org.redisson.connection.ConnectionManager;
import org.redisson.connection.MasterSlaveEntry;

/**
 *
 * @author Nikita Koksharov
 *
 */
public interface CommandAsyncExecutor {

    ConnectionManager getConnectionManager();

    CommandAsyncExecutor enableRedissonReferenceSupport(RedissonClient redisson);
    
    CommandAsyncExecutor enableRedissonReferenceSupport(RedissonReactiveClient redissonReactive);
    
    CommandAsyncExecutor enableRedissonReferenceSupport(RedissonRxClient redissonReactive);
    
    boolean isRedissonReferenceSupportEnabled();
    
    <V> RedisException convertException(RFuture<V> RFuture);

    boolean await(RFuture<?> RFuture, long timeout, TimeUnit timeoutUnit) throws InterruptedException;
    
    void syncSubscription(RFuture<?> future);
    
    <V> V get(RFuture<V> RFuture);

    <T, R> RFuture<R> writeAsync(MasterSlaveEntry entry, Codec codec, RedisCommand<T> command, Object ... params);
    
    <T, R> RFuture<R> readAsync(RedisClient client, MasterSlaveEntry entry, Codec codec, RedisCommand<T> command, Object ... params);
    
    <T, R> RFuture<R> readAsync(RedisClient client, String name, Codec codec, RedisCommand<T> command, Object ... params);
    
    <T, R> RFuture<R> readAsync(RedisClient client, byte[] key, Codec codec, RedisCommand<T> command, Object... params);
    
    <T, R> RFuture<R> readAsync(RedisClient client, Codec codec, RedisCommand<T> command, Object ... params);

    <T, R> RFuture<R> evalWriteAllAsync(RedisCommand<T> command, SlotCallback<T, R> callback, String script, List<Object> keys, Object ... params);

    <R, T> RFuture<R> writeAllAsync(RedisCommand<T> command, SlotCallback<T, R> callback, Object ... params);

    <R, T> RFuture<R> readAllAsync(RedisCommand<T> command, SlotCallback<T, R> callback, Object ... params);

    <T, R> RFuture<R> evalReadAsync(RedisClient client, String name, Codec codec, RedisCommand<T> evalCommandType, String script, List<Object> keys, Object ... params);

    <T, R> RFuture<R> evalReadAsync(String key, Codec codec, RedisCommand<T> evalCommandType, String script, List<Object> keys, Object ... params);

    <T, R> RFuture<R> evalReadAsync(MasterSlaveEntry entry, Codec codec, RedisCommand<T> evalCommandType, String script, List<Object> keys, Object ... params);

    /**
     * EVAL SCRIPT KEY_NUM KEY1 KEY2 ... KEYN ARG1 ARG2 ....
     * 在 lua 脚本中，数组下标是从 1 开始，所以通过 KEYS[1] 就可以得到 第一个 key，通过 ARGV[1] 就可以得到第一个附加参数
     * @param key 感觉没有用
     * @param codec
     * @param evalCommandType
     * @param script 对应eval命令入参script
     * @param keys 对应eval命令入参key1,key2...
     * @param params 对应eval命令入参arg1,arg2...
     * @param <T>
     * @param <R>
     * @return
     */
    <T, R> RFuture<R> evalWriteAsync(String key, Codec codec, RedisCommand<T> evalCommandType, String script, List<Object> keys, Object ... params);

    <T, R> RFuture<R> evalWriteAsync(MasterSlaveEntry entry, Codec codec, RedisCommand<T> evalCommandType, String script, List<Object> keys, Object ... params);
    
    <T, R> RFuture<R> readAsync(byte[] key, Codec codec, RedisCommand<T> command, Object... params);
    
    <T, R> RFuture<R> readAsync(String key, Codec codec, RedisCommand<T> command, Object ... params);

    <T, R> RFuture<R> writeAsync(String key, Codec codec, RedisCommand<T> command, Object ... params);

    <T, R> RFuture<Collection<R>> readAllAsync(RedisCommand<T> command, Object ... params);
    
    <T, R> RFuture<Collection<R>> readAllAsync(Collection<R> results, RedisCommand<T> command, Object ... params);

    <R, T> RFuture<R> writeAllAsync(Codec codec, RedisCommand<T> command, SlotCallback<T, R> callback, Object... params);
    
    <T> RFuture<Void> writeAllAsync(RedisCommand<T> command, Object ... params);

    <T, R> RFuture<R> writeAsync(String key, RedisCommand<T> command, Object ... params);

    <T, R> RFuture<R> readAsync(String key, RedisCommand<T> command, Object ... params);

    <T, R> RFuture<R> readAsync(MasterSlaveEntry entry, Codec codec, RedisCommand<T> command, Object ... params);
    
    <T, R> RFuture<R> readRandomAsync(Codec codec, RedisCommand<T> command, Object ... params);
    
    <T, R> RFuture<R> readRandomAsync(MasterSlaveEntry entry, Codec codec, RedisCommand<T> command, Object... params);

}
