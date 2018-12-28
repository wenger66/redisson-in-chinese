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
package org.redisson;

import java.util.Arrays;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.redisson.api.RCountDownLatch;
import org.redisson.api.RFuture;
import org.redisson.client.codec.LongCodec;
import org.redisson.client.protocol.RedisCommands;
import org.redisson.command.CommandAsyncExecutor;
import org.redisson.pubsub.CountDownLatchPubSub;

/**
 * 分布式的倒计时闩锁实现 {@link java.util.concurrent.CountDownLatch}
 * 相比JDK实现的优点是计数可以重置 {@link #trySetCount}
 * -- 感觉没有这个优点，可以参考{@link #trySetCount}的注释
 * 这个对象没有继承RedissonExpirable，因为会在{@link #countDown()}达到0时删除
 *
 * @author Nikita Koksharov
 *
 */
public class RedissonCountDownLatch extends RedissonObject implements RCountDownLatch {

    public static final Long zeroCountMessage = 0L;
    public static final Long newCountMessage = 1L;

    private static final CountDownLatchPubSub PUBSUB = new CountDownLatchPubSub();

    private final UUID id;

    protected RedissonCountDownLatch(CommandAsyncExecutor commandExecutor, String name) {
        super(commandExecutor, name);
        this.id = commandExecutor.getConnectionManager().getId();
    }

    public void await() throws InterruptedException {
        RFuture<RedissonCountDownLatchEntry> future = subscribe();
        try {
            commandExecutor.syncSubscription(future);

            while (getCount() > 0) {
                // waiting for open state
                RedissonCountDownLatchEntry entry = getEntry();
                if (entry != null) {
                    entry.getLatch().await();
                }
            }
        } finally {
            unsubscribe(future);
        }
    }

    /**
     *
     * @param time
     * @param unit the time unit of the {@code timeout} argument
     * @return
     * @throws InterruptedException
     */
    @Override
    public boolean await(long time, TimeUnit unit) throws InterruptedException {
        long remainTime = unit.toMillis(time);
        long current = System.currentTimeMillis();
        RFuture<RedissonCountDownLatchEntry> promise = subscribe();
        if (!await(promise, time, unit)) {
            return false;
        }

        try {
            remainTime -= (System.currentTimeMillis() - current);
            if (remainTime <= 0) {
                return false;
            }

            while (getCount() > 0) {
                if (remainTime <= 0) {
                    return false;
                }
                current = System.currentTimeMillis();
                // waiting for open state
                RedissonCountDownLatchEntry entry = getEntry();
                if (entry != null) {
                    entry.getLatch().await(remainTime, TimeUnit.MILLISECONDS);
                }

                remainTime -= (System.currentTimeMillis() - current);
            }

            return true;
        } finally {
            unsubscribe(promise);
        }
    }

    private RedissonCountDownLatchEntry getEntry() {
        return PUBSUB.getEntry(getEntryName());
    }

    private RFuture<RedissonCountDownLatchEntry> subscribe() {
        return PUBSUB.subscribe(getEntryName(), getChannelName(), commandExecutor.getConnectionManager().getSubscribeService());
    }

    private void unsubscribe(RFuture<RedissonCountDownLatchEntry> future) {
        PUBSUB.unsubscribe(future.getNow(), getEntryName(), getChannelName(), commandExecutor.getConnectionManager().getSubscribeService());
    }

    /**
     * 倒计时的同步实现
     */
    @Override
    public void countDown() {
        get(countDownAsync());
    }

    /**
     * 倒计时的异步实现
     *
     * <p>
     *     实现方法：倒计时键是一个String，countdown通过lua脚本实现。
     *     1 - <code>decr key</code>, 递减计数，返回当前计数
     *     2 - 如果当前计数是0，<code>del key</code>，删除计数键
     *     3 - 如果当前计数是0，<code>publish channel message</code>，发布倒计时闩锁消息
     * </p>
     * <p>
     *     命令参数：
     *     key - 倒计时键名
     *     channel - 倒计时闩锁的监听通道："redisson_countdownlatch__channel__{" + getName() + "}"
     *     message - 倒计时清0的消息：zeroCountMessage
     * </p>
     *
     * @return
     */
    @Override
    public RFuture<Void> countDownAsync() {
        return commandExecutor.evalWriteAsync(getName(), LongCodec.INSTANCE, RedisCommands.EVAL_BOOLEAN,
                        "local v = redis.call('decr', KEYS[1]);" +
                        "if v <= 0 then redis.call('del', KEYS[1]) end;" +
                        "if v == 0 then redis.call('publish', KEYS[2], ARGV[1]) end;",
                    Arrays.<Object>asList(getName(), getChannelName()), zeroCountMessage);
    }

    private String getEntryName() {
        return id + getName();
    }

    private String getChannelName() {
        return "redisson_countdownlatch__channel__{" + getName() + "}";
    }

    /**
     * 获取当前倒计时值的同步实现
     * @return
     */
    @Override
    public long getCount() {
        return get(getCountAsync());
    }

    /**
     * 获取当前倒计时值的异步实现
     * <p>
     *     实现方法：倒计时键是一个String。getCount通过获取命令实现
     *     1 - <code>get key</code>，获取倒计时值
     * </p>
     *
     * @return
     */
    @Override
    public RFuture<Long> getCountAsync() {
        return commandExecutor.writeAsync(getName(), LongCodec.INSTANCE, RedisCommands.GET_LONG, getName());
    }

    /**
     * 设置倒计时值的同步实现
     * 必须 {@link #countDown} count的次数后，门闩才被打开，线程才可以通过
     * @return
     */
    @Override
    public boolean trySetCount(long count) {
        return get(trySetCountAsync(count));
    }

    /**
     * 设置倒计时值的异步实现
     * 可以在倒计时值达到0时或者倒计时键不存在时设置
     * -- 感觉作者描述错误，只有倒计时键不存在时才可以设置，以下命令证明，键值为0，返回存在且返回值为1
     * 127.0.0.1:6379> set test 0
     * OK
     * 127.0.0.1:6379> exists test
     * (integer) 1
     *
     * <p>
     *     实现方法：倒计时键是一个String，trySetCount通过lua脚本实现。
     *     1 - <code>exists key</code>, 判断倒计时键是否存在
     *     2 - 如果不存在，<code>set key value</code>,设置倒计时值；<code>publish channel message</code>，发布倒计时闩锁消息；返回1
     *     3 - 如果存在，返回0
     * </p>
     * <p>
     *     命令参数：
     *     key - 倒计时键名
     *     value - 倒计时值
     *     channel - 倒计时闩锁的监听通道："redisson_countdownlatch__channel__{" + getName() + "}"
     *     message - 1：newCountMessage
     * </p>
     *
     * @param count - 必须 {@link #countDown} count的次数后，门闩才被打开，线程才可以通过
     * @return 如果设置倒计时键成功，返回<code>true</code>
     *         如果倒计时键已存在，返回<code>false</code>
     */
    @Override
    public RFuture<Boolean> trySetCountAsync(long count) {
        return commandExecutor.evalWriteAsync(getName(), LongCodec.INSTANCE, RedisCommands.EVAL_BOOLEAN,
                "if redis.call('exists', KEYS[1]) == 0 then "
                    + "redis.call('set', KEYS[1], ARGV[2]); "
                    + "redis.call('publish', KEYS[2], ARGV[1]); "
                    + "return 1 "
                + "else "
                    + "return 0 "
                + "end",
                Arrays.<Object>asList(getName(), getChannelName()), newCountMessage, count);
    }

    /**
     * 覆盖Redisson对象删除键的异步实现
     * 与普通的删除键不同的是
     * 删除了倒计时键后，相当于打开了闩锁，需要发布倒计时闩锁的消息
     *
     * @return
     */
    @Override
    public RFuture<Boolean> deleteAsync() {
        return commandExecutor.evalWriteAsync(getName(), LongCodec.INSTANCE, RedisCommands.EVAL_BOOLEAN,
                "if redis.call('del', KEYS[1]) == 1 then "
                    + "redis.call('publish', KEYS[2], ARGV[1]); "
                    + "return 1 "
                + "else "
                    + "return 0 "
                + "end",
                Arrays.<Object>asList(getName(), getChannelName()), newCountMessage);
    }

}
