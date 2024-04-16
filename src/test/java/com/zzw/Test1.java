package com.zzw;

import com.zzw.p0_core.RingBuffer;
import com.zzw.p0_core.Sequence;
import com.zzw.p3_consumer.OrderEventHandler;
import com.zzw.p3_consumer.BatchEventProcessor;
import com.zzw.p2_barrier.SequenceBarrier;
import com.zzw.p2_barrier.wait.BlockingWaitStrategy;
import com.zzw.p1_producer.OrderEvent;
import com.zzw.p1_producer.OrderEventFactory;

public class Test1
{

    public static void main(String[] args)
    {
        int ringBufferSize = 16;
        RingBuffer<OrderEvent> ringBuffer = RingBuffer.createSingleProducer(
                new OrderEventFactory(),
                ringBufferSize,
                new BlockingWaitStrategy()
        );

        // 获得 RingBuffer 的消费序号屏障(v1 版本只监控生产序号)
        SequenceBarrier barrier = ringBuffer.newBarrier();
        // 基于消费序号屏障(用于控制消费速度), 创建消费者
        BatchEventProcessor<OrderEvent> eventProcessor = new BatchEventProcessor<>(
                ringBuffer,
                new OrderEventHandler(),
                barrier);
        // 向 RingBuffer 注册需要监控的消费序号, 用于控制生产速度
        Sequence sequence = eventProcessor.getSequence();
        ringBuffer.addGatingSequence(sequence);

        // 启动消费者线程
        new Thread(eventProcessor).start();

        // 生产者发布 100 个事件
        for (int i = 0; i < 100; i++)
        {
            long nextIndex = ringBuffer.next();

            OrderEvent orderEvent = ringBuffer.get(nextIndex);
            orderEvent.setMessage("message-" + i);
            orderEvent.setPrice(i * 10);

            System.out.println("生产者发布事件: " + orderEvent);
            ringBuffer.publish(nextIndex);
        }
    }
}
