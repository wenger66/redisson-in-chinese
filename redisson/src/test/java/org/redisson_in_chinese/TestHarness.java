package org.redisson_in_chinese;

import java.util.concurrent.CountDownLatch;

/**
 * TestHarness
 * <p/>
 * 使用CountDownLatch让所有工作线程同时开始（以准备资源），所有工作线程都结束后才总的结束（以统计并发工作的耗时）
 * 就好比赛马比赛一样，一声令下拉开马栓，比赛才开始，最后一匹马跑过终点后，比赛才结束
 * @author Brian Goetz and Tim Peierls
 */
public class TestHarness {

   public static void main(String[] args) throws InterruptedException {
       TestHarness testHarness = new TestHarness();
       Runnable task = new Runnable() {
           @Override
           public void run() {
               System.out.printf("run task %s%n", Thread.currentThread().getId());
           }
       };
       testHarness.timeTasks(10, task);
   }

    public long timeTasks(int nThreads, final Runnable task)
            throws InterruptedException {
        final CountDownLatch startGate = new CountDownLatch(1);
        final CountDownLatch endGate = new CountDownLatch(nThreads);

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
