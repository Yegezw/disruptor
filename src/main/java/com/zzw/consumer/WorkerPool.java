package com.zzw.consumer;

import com.zzw.collection.RingBuffer;
import com.zzw.relation.Sequence;
import com.zzw.relation.SequenceBarrier;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executor;

/**
 * 多线程消费者池
 */
public class WorkerPool<T> {

    private final RingBuffer<T> ringBuffer;

    // ----------------------------------------

    /**
     * <p>工作消费序号, 多线程消费者共同的已申请序号(可能未消费)
     * <p>主要用于通过 CAS 协调同一 WorkerPool 内消费者线程争抢序号
     */
    private final Sequence workSequence = new Sequence(-1);
    /**
     * "多线程消费者的工作线程" 列表
     */
    private final List<WorkProcessor<T>> workProcessorList;

    // =============================================================================

    public WorkerPool(RingBuffer<T> ringBuffer, SequenceBarrier sequenceBarrier, WorkHandler<T>... workHandlerList) {
        this.ringBuffer = ringBuffer;
        final int numWorkers = workHandlerList.length;
        this.workProcessorList = new ArrayList<>(numWorkers);

        // 为每个自定义事件消费逻辑 EventHandler, 创建一个对应的 WorkProcessor 去处理
        for (WorkHandler<T> eventConsumer : workHandlerList) {
            workProcessorList.add(new WorkProcessor<>(
                    ringBuffer,
                    eventConsumer,
                    sequenceBarrier,
                    this.workSequence)
            );
        }
    }

    // =============================================================================

    /**
     * 返回 workerPool + workerEventProcessor 的序号集合
     */
    public Sequence[] getCurrentWorkerSequences() {
        final Sequence[] sequences = new Sequence[this.workProcessorList.size() + 1];
        for (int i = 0, size = workProcessorList.size(); i < size; i++) {
            sequences[i] = workProcessorList.get(i).getCurrentConsumeSequence();
        }
        sequences[sequences.length - 1] = workSequence;

        return sequences;
    }

    /**
     * 启动多线程消费者
     */
    public RingBuffer<T> start(final Executor executor) {
        // 生产者序号
        final long cursor = ringBuffer.getCurrentProducerSequence().get();

        // 设置 workSequence = cursor
        workSequence.set(cursor);

        // 设置 WorkProcessor.currentConsumeSequence = cursor
        for (WorkProcessor<?> processor : workProcessorList) {
            processor.getCurrentConsumeSequence().set(cursor);
            executor.execute(processor);
        }

        return this.ringBuffer;
    }
}
