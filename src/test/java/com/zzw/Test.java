package com.zzw;

import com.zzw.collection.OrderEvent;
import com.zzw.collection.OrderEventFactory;
import com.zzw.collection.RingBuffer;
import com.zzw.consumer.BatchEventProcessor;
import com.zzw.consumer.OrderEventHandler;
import com.zzw.relation.Sequence;
import com.zzw.relation.SequenceBarrier;
import com.zzw.relation.wait.BlockingWaitStrategy;

public class Test {

    public static void main(String[] args) {
        int ringBufferSize = 16;
        RingBuffer<OrderEvent> ringBuffer = RingBuffer.createSingleProducer(
                new OrderEventFactory(),
                ringBufferSize,
                new BlockingWaitStrategy()
        );

        // 获得 RingBuffer 的序号屏障(v1 版本只维护生产者的序号)
        SequenceBarrier sequenceBarrier = ringBuffer.newBarrier();
        // 基于序号屏障, 创建消费者
        BatchEventProcessor<OrderEvent> eventProcessor = new BatchEventProcessor<>(
                ringBuffer,
                new OrderEventHandler(),
                sequenceBarrier);
        // RingBuffer 设置消费者的序列, 用于控制生产速度
        Sequence consumeSequence = eventProcessor.getCurrentConsumeSequence();
        ringBuffer.setConsumerSequence(consumeSequence);

        // 启动消费者线程
        new Thread(eventProcessor).start();

        // 生产者发布 100 个事件
        for (int i = 0; i < 100; i++) {
            long nextIndex = ringBuffer.next();

            OrderEvent orderEvent = ringBuffer.get(nextIndex);
            orderEvent.setMessage("message-" + i);
            orderEvent.setPrice(i * 10);

            System.out.println("生产者发布事件: " + orderEvent);
            ringBuffer.publish(nextIndex);
        }
    }
}
