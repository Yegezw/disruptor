package com.zzw;

import com.zzw.collection.OrderEvent;
import com.zzw.collection.OrderEventFactory;
import com.zzw.collection.RingBuffer;
import com.zzw.collection.dsl.Disruptor;
import com.zzw.collection.dsl.EventHandlerGroup;
import com.zzw.collection.dsl.producer.ProducerType;
import com.zzw.consumer.OrderEventHandler;
import com.zzw.relation.wait.BlockingWaitStrategy;
import com.zzw.util.Util;

import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class Test5
{

    private static Disruptor<OrderEvent> getDisruptor()
    {
        ThreadPoolExecutor pool = new ThreadPoolExecutor(
                10,
                10, 0L, TimeUnit.MICROSECONDS,
                new SynchronousQueue<>(),
                r ->
                {
                    Thread thread = new Thread(r);
                    thread.setDaemon(true); // 注意
                    return thread;
                }
        );
        return new Disruptor<>(
                new OrderEventFactory(),
                128,
                pool,
                ProducerType.SINGLE,
                new BlockingWaitStrategy()
        );
    }

    //             / -> [B & C] -> D
    // Event -> A -         
    //             \ ->    E    -> F
    public static void main(String[] args)
    {
        Disruptor<OrderEvent> disruptor = getDisruptor();

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

        // ======================================================================================

        // 启动消费者线程
        disruptor.start();

        // 生产者发布 100 个事件
        RingBuffer<OrderEvent> ringBuffer = disruptor.getRingBuffer();
        for (int i = 0; i < 100; i++)
        {
            long nextIndex = ringBuffer.next();

            OrderEvent orderEvent = ringBuffer.get(nextIndex);
            orderEvent.setMessage("message-" + i);
            orderEvent.setPrice(i * 10);

            System.out.println("生产者发布事件: " + orderEvent);
            ringBuffer.publish(nextIndex);
        }

        // 等所有消费者线程 "把已生产的事件全部消费完成" 后, 停止所有消费者线程
        // 因为生产者已将发布了 100 个事件, 因此消费者链条中的每个消费者都会消费完 100 个事件
        disruptor.shutdown();

        Util.sleep(1000);
        System.out.println("disruptor shutdown");
    }
}
