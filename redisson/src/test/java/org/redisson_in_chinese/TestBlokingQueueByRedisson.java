package org.redisson_in_chinese;

import org.redisson.Redisson;
import org.redisson.RedissonShutdownException;
import org.redisson.api.RBlockingQueue;
import org.redisson.api.RedissonClient;
import org.redisson.client.RedisClient;
import org.redisson.client.RedisClientConfig;
import org.redisson.config.Config;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class TestBlokingQueueByRedisson {
    /**
     * 一个线程不断向BlockQueue放数据
     * 一个线程不断从BlockQueue取数据
     * @param args
     * @throws InterruptedException
     */
    public static void main(String[] args) throws InterruptedException {
        TestBlokingQueueByRedisson testBlockingQueue = new TestBlokingQueueByRedisson();
        testBlockingQueue.setup();
        testBlockingQueue.consume();
        testBlockingQueue.provide();
        testBlockingQueue.shutdown();
    }

    private RedissonClient redissonClient;
    private RBlockingQueue<String> blockingQueue;
    // 测试推送个数
    private static final int TEST_SIZE = 10;
    // 测试时间，单位秒
    private static final int TEST_TIME = 300;
    // 倒计时闩锁
    private static final CountDownLatch endGate = new CountDownLatch(TEST_TIME);
    private static final ScheduledExecutorService scheduledThreadPool = Executors.newScheduledThreadPool(1);

    /**
     * 服务提供者只offer10个数字，必现org.redisson.RedissonShutdownException: Redisson is shutdown
     * 服务提供者只offer100个数字，虽然不出现RedissonShutdownException，但是使用redis-cli，rpush一个数字，redisson也拿不下来
     */

    private void setup() {
        final Config config2 = new Config();
        config2.useSingleServer()
//                .setAddress("redis://10.42.5.212:6379");
                .setPassword("db10$ZTE")
         .setAddress("redis://10.42.6.125:26379");
        config2.setCodec(new org.redisson.client.codec.StringCodec());

        redissonClient = Redisson.create(config2);

        blockingQueue = redissonClient.getBlockingQueue("testylw");
        blockingQueue.clear();

        scheduledThreadPool.scheduleAtFixedRate(new Runnable() {
            public void run() {
                endGate.countDown();
            }
        }, 0, 1, TimeUnit.SECONDS);
    }

    private void provide() {
        Thread t = new Thread("provide_thread") {
            public void run() {
                for( int i=0;i<TEST_SIZE;i++) {
                    try {
                        blockingQueue.offer(String.valueOf(i));
                        System.out.println("success to put "+i+" to blockingQueue");
                        Thread.sleep(100);
                    }catch(Throwable e) {

                    }
                }
            }
        };
        t.start();
    }

    private void consume() {
        Thread t = new Thread("consume_thread") {
            public void run() {
                while(true) {
                    try {
                        String value = blockingQueue.take();
                        System.out.println("success to take "+value+" from blockingQueue");
                        Thread.sleep(100);
                    }
                    catch(RedissonShutdownException e) {
                        e.printStackTrace();
//                        System.out.println("reconnect redis");
                        blockingQueue = redissonClient.getBlockingQueue("testylw");
                        try {
                            Thread.sleep(10000);
                        } catch (InterruptedException e1) {
                            e1.printStackTrace();
                        }
                    }
                    catch(Throwable e) {
                        e.printStackTrace();
                    }
                }

            }
        };
        t.start();
    }

    private void shutdown() {
        try {
            endGate.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        redissonClient.shutdown();
    }


}
