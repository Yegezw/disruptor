package com.zzw;

import com.zzw.collection.OrderEvent;
import com.zzw.collection.OrderEventFactory;
import com.zzw.collection.RingBuffer;
import com.zzw.collection.dsl.Disruptor;
import com.zzw.collection.dsl.EventHandlerGroup;
import com.zzw.collection.dsl.producer.ProducerType;
import com.zzw.consumer.OrderEventHandler;
import com.zzw.producer.OrderEventTranslator;
import com.zzw.relation.wait.BlockingWaitStrategy;

import java.util.concurrent.Executors;

public class Test1
{

    //                   / -> C -> E
    // Event -> [A & B] -                  -> H
    //                   \ -> D -> [F & G]
    public static void main(String[] args)
    {
        Disruptor<OrderEvent> disruptor = new Disruptor<>(
                new OrderEventFactory(),
                128,
                Executors.defaultThreadFactory(),
                ProducerType.SINGLE,
                new BlockingWaitStrategy()
        );

        // ======================================================================================

        OrderEventHandler a = new OrderEventHandler("consumerA");
        OrderEventHandler b = new OrderEventHandler("consumerB");
        OrderEventHandler c = new OrderEventHandler("consumerC");
        OrderEventHandler d = new OrderEventHandler("consumerD");
        OrderEventHandler e = new OrderEventHandler("consumerE");
        OrderEventHandler f = new OrderEventHandler("consumerF");
        OrderEventHandler g = new OrderEventHandler("consumerG");
        OrderEventHandler h = new OrderEventHandler("consumerH");

        disruptor.handleEventsWith(a, b);
        EventHandlerGroup<OrderEvent> groupAB = disruptor.after(a, b);
        groupAB.handleEventsWith(c).then(e);
        groupAB.handleEventsWith(d).then(f, g);
        disruptor.after(e, f, g).handleEventsWith(h);

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
         *         {name = pool-1-thread-7, id = 26, state = TIMED_WAITING, lockInfo = null}
         *         {name = pool-1-thread-8, id = 27, state = TIMED_WAITING, lockInfo = null}
         *     }
         * }
         */
        System.out.println(disruptor);

        // ======================================================================================

        RingBuffer<OrderEvent> ringBuffer = disruptor.getRingBuffer();
        OrderEventTranslator   translator = new OrderEventTranslator();
        // 生产者发布 10 个事件
        for (int i = 0; i < 10; i++)
        {
            ringBuffer.publishEvent(translator);
        }
        // 生产者发布 10 个事件
        ringBuffer.publishEvents(translator, new Object[10][]);

        // 关闭 disruptor
        disruptor.shutdown();
    }
}
