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

public class Test6
{

    //             / -> [B & C] -> D
    // Event -> A -         
    //             \ ->    E    -> F
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

        EventHandlerGroup<OrderEvent> consumerA = disruptor.handleEventsWith(a);

        consumerA.then(b, c).then(d);
        consumerA.then(e).then(f);

        // 启动消费者线程
        disruptor.start();

        /*
         * Disruptor {
         *     ringBuffer = RingBuffer {
         *         bufferSize = 128,
         *         sequencer = AbstractSequencer {
         *             waitStrategy = com.zzw.relation.wait.BlockingWaitStrategy,
         *             cursor = -1,
         *             gatingSequences = [-1, -1]
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

        RingBuffer<OrderEvent> ringBuffer = disruptor.getRingBuffer();
        OrderEventTranslator   translator = new OrderEventTranslator();
        // 生产者发布 100 个事件
        for (int i = 0; i < 100; i++)
        {
            ringBuffer.publishEvent(translator);
        }
        // 生产者发布 100 个事件
        ringBuffer.publishEvents(translator, new Object[100][]);

        // ======================================================================================

        // 等所有消费者线程 "把已生产的事件全部消费完成" 后, 停止所有消费者线程
        // 因为生产者已将发布了 100 个事件, 因此消费者链条中的每个消费者都会消费完 100 个事件
        disruptor.shutdown();

        System.out.println("disruptor shutdown");
    }
}
