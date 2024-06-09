package com.zzw;

import com.zzw.collection.OrderEvent;
import com.zzw.collection.OrderEventFactory;
import com.zzw.collection.RingBuffer;
import com.zzw.collection.dsl.ExceptionHandlerWrapper;
import com.zzw.consumer.BatchEventProcessor;
import com.zzw.consumer.OrderEventHandler;
import com.zzw.consumer.OrderWorkHandler;
import com.zzw.consumer.WorkerPool;
import com.zzw.relation.Sequence;
import com.zzw.relation.SequenceBarrier;
import com.zzw.relation.wait.BlockingWaitStrategy;

import java.util.concurrent.Executors;

public class Test2
{

    //                / -> 多 B \
    // Event -> 单 A -           -> 单 D
    //                \ -> 单 C /
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
                sequenceBarrier,
                new OrderEventHandler("consumerA"));
        // RingBuffer 监听消费者 A 的序号, 用于控制生产速度
        Sequence consumeSequenceA = eventProcessorA.getSequence();
        ringBuffer.addGatingSequences(consumeSequenceA);

        // --------------------------------------------------------------------------------------

        // "多线程消费者 B" 和 "单线程消费者 C"
        // 都依赖上游消费者 A, 通过消费者 A 的序号创建序号屏障, 构成消费的顺序依赖
        SequenceBarrier sequenceBarrierA = ringBuffer.newBarrier(consumeSequenceA);

        // 基于序号屏障, 创建多线程消费者 B
        WorkerPool<OrderEvent> workPoolB = new WorkerPool<>(
                ringBuffer,
                sequenceBarrierA,
                new ExceptionHandlerWrapper<>(),
                new OrderWorkHandler("consumerB1"),
                new OrderWorkHandler("consumerB2"),
                new OrderWorkHandler("consumerB3")
        );
        // RingBuffer 监听多线程消费者 B 的序号, 用于控制生产速度
        Sequence[] consumeSequenceB = workPoolB.getWorkerSequences();
        ringBuffer.addGatingSequences(consumeSequenceB);

        // 基于屏障, 创建消费者 C
        BatchEventProcessor<OrderEvent> eventProcessorC = new BatchEventProcessor<>(
                ringBuffer,
                sequenceBarrierA,
                new OrderEventHandler("consumerC"));
        // RingBuffer 监听消费者 C 的序号, 用于控制生产速度
        Sequence consumeSequenceC = eventProcessorC.getSequence();
        ringBuffer.addGatingSequences(consumeSequenceC);

        // --------------------------------------------------------------------------------------

        Sequence[] consumeSequenceBC = new Sequence[consumeSequenceB.length + 1];
        System.arraycopy(consumeSequenceB, 0, consumeSequenceBC, 0, consumeSequenceB.length);
        consumeSequenceBC[consumeSequenceBC.length - 1] = consumeSequenceC;

        // 消费者 D 依赖上游消费者 B C, 通过消费者 B C 的序号创建序号屏障, 构成消费的顺序依赖
        SequenceBarrier sequenceBarrierBC = ringBuffer.newBarrier(consumeSequenceBC);

        // 基于序号屏障, 创建消费者 D
        BatchEventProcessor<OrderEvent> eventProcessorD = new BatchEventProcessor<>(
                ringBuffer,
                sequenceBarrierBC,
                new OrderEventHandler("consumerD"));
        // RingBuffer 监听消费者 D 的序号, 用于控制生产速度
        Sequence consumeSequenceD = eventProcessorD.getSequence();
        ringBuffer.addGatingSequences(consumeSequenceD);

        // ======================================================================================

        // 启动消费者线程
        new Thread(eventProcessorA).start(); // A
        workPoolB.start(Executors.newFixedThreadPool(3)); // B
        new Thread(eventProcessorC).start(); // C
        new Thread(eventProcessorD).start(); // D

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
