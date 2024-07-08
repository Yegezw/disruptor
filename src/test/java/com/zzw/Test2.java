package com.zzw;

import com.zzw.collection.OrderEvent;
import com.zzw.collection.OrderEventFactory;
import com.zzw.collection.RingBuffer;
import com.zzw.collection.dsl.Disruptor;
import com.zzw.collection.dsl.EventHandlerGroup;
import com.zzw.collection.dsl.producer.ProducerType;
import com.zzw.consumer.OrderEventHandler;
import com.zzw.consumer.OrderWorkHandler;
import com.zzw.producer.OrderEventTranslator;
import com.zzw.relation.wait.BlockingWaitStrategy;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

public class Test2
{

    //                / -> 单 B
    // Event -> 单 A -          -> 单 D
    //                \ -> 多 C
    public static void main(String[] args) throws Exception
    {
        Disruptor<OrderEvent> disruptor = new Disruptor<>(
                new OrderEventFactory(),
                128,
                Executors.defaultThreadFactory(),
                ProducerType.MULTI,
                new BlockingWaitStrategy()
        );

        // ======================================================================================

        OrderEventHandler a = new OrderEventHandler("consumerA");
        OrderEventHandler b = new OrderEventHandler("consumerC");
        OrderEventHandler d = new OrderEventHandler("consumerD");

        OrderWorkHandler c1 = new OrderWorkHandler("consumerB1");
        OrderWorkHandler c2 = new OrderWorkHandler("consumerB2");
        OrderWorkHandler c3 = new OrderWorkHandler("consumerB3");

        disruptor.handleEventsWith(a);
        EventHandlerGroup<OrderEvent> groupA = disruptor.after(a);
        EventHandlerGroup<OrderEvent> groupB = groupA.handleEventsWith(b);
        EventHandlerGroup<OrderEvent> groupC = groupA.handleEventsWithWorkerPool(c1, c2, c3);
        groupB.and(groupC).handleEventsWith(d);

        // 启动消费者线程
        disruptor.start();
        /*
         * Disruptor {
         *     ringBuffer = RingBuffer {
         *         bufferSize = 128,
         *         sequencer = AbstractSequencer {
         *             waitStrategy = com.zzw.relation.wait.BlockingWaitStrategy,
         *             cursor = -1,
         *             gatingSequences = [-1]
         *         }
         *     },
         *     started = true,
         *     executor = BasicExecutor {
         *         threads =
         *         {name = pool-1-thread-1, id = 20, state = TIMED_WAITING, lockInfo = null}
         *         {name = pool-1-thread-2, id = 21, state = TIMED_WAITING, lockInfo = null}
         *         {name = pool-1-thread-3, id = 22, state = TIMED_WAITING, lockInfo = null}
         *         {name = pool-1-thread-4, id = 23, state = TIMED_WAITING, lockInfo = null}
         *         {name = pool-1-thread-5, id = 24, state = TIMED_WAITING, lockInfo = null}
         *         {name = pool-1-thread-6, id = 25, state = TIMED_WAITING, lockInfo = null}
         *     }
         * }
         */
        System.out.println(disruptor);

        // ======================================================================================

        // 生产者线程池
        ExecutorService pool = Executors.newFixedThreadPool(3, new ThreadFactory()
        {
            private final AtomicInteger count = new AtomicInteger(1);

            @Override
            public Thread newThread(Runnable r)
            {
                return new Thread(r, "workerProducer-" + count.getAndIncrement());
            }
        });
        RingBuffer<OrderEvent> ringBuffer = disruptor.getRingBuffer();
        OrderEventTranslator   translator = new OrderEventTranslator();

        // 启动 3 个生产者线程
        CountDownLatch latch = new CountDownLatch(3);
        for (int i = 0; i < 3; i++)
        {
            pool.execute(
                    () ->
                    {
                        // 生产者发布 10 个事件
                        ringBuffer.publishEvents(translator, new Object[10][]);
                        latch.countDown();
                    }
            );
        }

        latch.await();
        disruptor.shutdown();
        pool.shutdownNow(); // 关闭生产者线程池
    }
}
