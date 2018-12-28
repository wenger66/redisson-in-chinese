package org.redisson_in_chinese;

import org.redisson.Redisson;
import org.redisson.api.RCountDownLatch;
import org.redisson.api.RedissonClient;
import org.redisson.client.*;
import org.redisson.client.codec.StringCodec;
import org.redisson.client.protocol.pubsub.PubSubType;
import org.redisson.config.Config;

/**
 * TestHarnessByRedisson
 * <p/>
 * 使用Redisson实现TestHarness
 * @author wenger66
 */
public class TestHarnessByRedisson {

    public static void main(String[] args) throws InterruptedException {
        TestHarnessByRedisson testHarness = new TestHarnessByRedisson();

        Runnable task = new Runnable() {
            @Override
            public void run() {
                System.out.printf("run task %s%n", Thread.currentThread().getId());
            }
        };
        testHarness.setup();
        testHarness.subscribe();
        testHarness.timeTasks(10, task);
        testHarness.shutdown();
    }

    private RedisClient redisClient;
    private RedissonClient redissonClient;
    private RedisPubSubConnection pubSubConnection;

    public void setup() {
        RedisClientConfig config = new RedisClientConfig();
        config.setAddress("redis://10.42.5.212:6379");
        redisClient = RedisClient.create(config);

        final Config config2 = new Config();
        config2.useSingleServer()
                .setAddress("redis://10.42.5.212:6379");
        config2.setCodec(new org.redisson.client.codec.StringCodec());
        redissonClient = Redisson.create(config2);
    }

    public void shutdown() {
        redissonClient.shutdown();
        redisClient.shutdown();
    }

    public void subscribe() throws InterruptedException {
        pubSubConnection = redisClient.connectPubSub();
        pubSubConnection.addListener(new RedisPubSubListener<Object>() {
            @Override
            public boolean onStatus(PubSubType type, CharSequence channel) {
                System.out.printf("type:%s, channel:%s %n", type, channel);
                return true;
            }

            @Override
            public void onMessage(CharSequence channel, Object message) {
                System.out.printf("channel:%s, message:%s %n", channel, message);
            }

            @Override
            public void onPatternMessage(CharSequence pattern, CharSequence channel, Object message) {
            }
        });
        pubSubConnection.subscribe(StringCodec.INSTANCE, new ChannelName("redisson_countdownlatch__channel__{endGate}"), new ChannelName("redisson_countdownlatch__channel__{startGate}"));

    }

    public long timeTasks(int nThreads, final Runnable task)
            throws InterruptedException {

        final RCountDownLatch startGate = redissonClient.getCountDownLatch("startGate");
        startGate.trySetCount(1);
        final RCountDownLatch endGate = redissonClient.getCountDownLatch("endGate");
        endGate.trySetCount(nThreads);

        for (int i = 0; i < nThreads; i++) {
            Thread t = new Thread() {
                public void run() {
                    try {
                        startGate.await();
                        try {
                            task.run();
                        } finally {
                            endGate.countDown();
                        }
                    } catch (InterruptedException ignored) {
                    }
                }
            };
            t.start();
            System.out.printf("start task %s%n", t.getId());
        }

        long start = System.currentTimeMillis();
        startGate.countDown();
        endGate.await();
        long end = System.currentTimeMillis();
        System.out.printf("end all task, use %d ms %n", end - start);
        return end - start;
    }
}
