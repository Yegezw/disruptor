package com.zzw;

import com.zzw.collection.OrderEvent;
import com.zzw.collection.OrderEventFactory;
import com.zzw.collection.RingBuffer;
import com.zzw.consumer.BatchEventProcessor;
import com.zzw.consumer.OrderEventHandler;
import com.zzw.relation.Sequence;
import com.zzw.relation.SequenceBarrier;
import com.zzw.relation.wait.BlockingWaitStrategy;

public class Test1
{

    //                   / -> C -> E
    // Event -> [A & B] -
    //                   \ -> D -> [F & G]
    public static void main(String[] args)
    {
        int ringBufferSize = 16;
        RingBuffer<OrderEvent> ringBuffer = RingBuffer.createSingleProducer(
                new OrderEventFactory(),
                ringBufferSize,
                new BlockingWaitStrategy()
        );

        // ======================================================================================

        // 获得 RingBuffer 的序号屏障(最上游的序号屏障内只维护生产者的序号)
        SequenceBarrier sequenceBarrier = ringBuffer.newBarrier();

        // 基于序号屏障, 创建消费者 A
        BatchEventProcessor<OrderEvent> eventProcessorA = new BatchEventProcessor<>(
                ringBuffer,
                new OrderEventHandler("consumerA"),
                sequenceBarrier);
        // RingBuffer 监听消费者 A 的序号, 用于控制生产速度
        Sequence consumeSequenceA = eventProcessorA.getCurrentConsumeSequence();
        ringBuffer.addGatingConsumerSequenceList(consumeSequenceA);

        // 基于序号屏障, 创建消费者 B
        BatchEventProcessor<OrderEvent> eventProcessorB = new BatchEventProcessor<>(
                ringBuffer,
                new OrderEventHandler("consumerB"),
                sequenceBarrier);
        // RingBuffer 监听消费者 B 的序号, 用于控制生产速度
        Sequence consumeSequenceB = eventProcessorB.getCurrentConsumeSequence();
        ringBuffer.addGatingConsumerSequenceList(consumeSequenceB);

        // --------------------------------------------------------------------------------------

        // 消费者 C 和 D 都依赖上游消费者 A B, 通过消费者 A B 的序号创建序号屏障, 构成消费的顺序依赖
        SequenceBarrier sequenceBarrierAB = ringBuffer.newBarrier(consumeSequenceA, consumeSequenceB);

        // 基于屏障, 创建消费者 C
        BatchEventProcessor<OrderEvent> eventProcessorC = new BatchEventProcessor<>(
                ringBuffer,
                new OrderEventHandler("consumerC"),
                sequenceBarrierAB);
        // RingBuffer 监听消费者 C 的序号, 用于控制生产速度
        Sequence consumeSequenceC = eventProcessorC.getCurrentConsumeSequence();
        ringBuffer.addGatingConsumerSequenceList(consumeSequenceC);

        // 基于序号屏障, 创建消费者 D
        BatchEventProcessor<OrderEvent> eventProcessorD = new BatchEventProcessor<>(
                ringBuffer,
                new OrderEventHandler("consumerD"),
                sequenceBarrierAB);
        // RingBuffer 监听消费者 D 的序号, 用于控制生产速度
        Sequence consumeSequenceD = eventProcessorD.getCurrentConsumeSequence();
        ringBuffer.addGatingConsumerSequenceList(consumeSequenceD);

        // --------------------------------------------------------------------------------------

        // 消费者 E 依赖上游消费者 C, 通过消费者 C 的序号创建序号屏障, 构成消费的顺序依赖
        SequenceBarrier sequenceBarrierC = ringBuffer.newBarrier(consumeSequenceC);

        // 基于序号屏障, 创建消费者 E
        BatchEventProcessor<OrderEvent> eventProcessorE = new BatchEventProcessor<>(
                ringBuffer,
                new OrderEventHandler("consumerE"),
                sequenceBarrierC);
        // RingBuffer 监听消费者 E 的序号, 用于控制生产速度
        Sequence consumeSequenceE = eventProcessorE.getCurrentConsumeSequence();
        ringBuffer.addGatingConsumerSequenceList(consumeSequenceE);

        // --------------------------------------------------------------------------------------

        // 消费者 F 和 G 都依赖上游消费者 D, 通过消费者 D 的序号创建序号屏障, 构成消费的顺序依赖
        SequenceBarrier sequenceBarrierD = ringBuffer.newBarrier(consumeSequenceD);

        // 基于序号屏障, 创建消费者 F
        BatchEventProcessor<OrderEvent> eventProcessorF = new BatchEventProcessor<>(
                ringBuffer,
                new OrderEventHandler("consumerF"),
                sequenceBarrierD);
        // RingBuffer 监听消费者 F 的序号, 用于控制生产速度
        Sequence consumeSequenceF = eventProcessorF.getCurrentConsumeSequence();
        ringBuffer.addGatingConsumerSequenceList(consumeSequenceF);

        // 基于序号屏障, 创建消费者 G
        BatchEventProcessor<OrderEvent> eventProcessorG = new BatchEventProcessor<>(
                ringBuffer,
                new OrderEventHandler("consumerG"),
                sequenceBarrierD);
        // RingBuffer 监听消费者 G 的序号, 用于控制生产速度
        Sequence consumeSequenceG = eventProcessorG.getCurrentConsumeSequence();
        ringBuffer.addGatingConsumerSequenceList(consumeSequenceG);

        // ======================================================================================

        // 启动消费者线程
        new Thread(eventProcessorA).start();
        new Thread(eventProcessorB).start();
        new Thread(eventProcessorC).start();
        new Thread(eventProcessorD).start();
        new Thread(eventProcessorE).start();
        new Thread(eventProcessorF).start();
        new Thread(eventProcessorG).start();

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
