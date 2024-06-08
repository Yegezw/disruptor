package com.zzw.consumer;

import com.zzw.collection.RingBuffer;
import com.zzw.relation.Sequence;
import com.zzw.relation.SequenceBarrier;

import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 多线程消费者池
 */
public class WorkerPool<T>
{

    private final RingBuffer<T> ringBuffer;
    private final AtomicBoolean started = new AtomicBoolean(false);

    // ----------------------------------------

    /**
     * <p>工作消费序号, 多线程消费者共同的已申请序号(可能未消费)
     * <p>主要用于通过 CAS 协调同一 WorkerPool 内消费者线程争抢序号
     */
    private final Sequence           workSequence = new Sequence(-1);
    /**
     * "多线程消费者的工作线程" 数组
     */
    private final WorkProcessor<?>[] workProcessors;

    // =============================================================================

    @SafeVarargs
    public WorkerPool(RingBuffer<T> ringBuffer,
                      SequenceBarrier sequenceBarrier,
                      WorkHandler<? super T>... workHandlers)
    {
        this.ringBuffer = ringBuffer;
        final int numWorkers = workHandlers.length;
        workProcessors = new WorkProcessor[numWorkers];

        // 为每个自定义事件消费逻辑 EventHandler, 创建一个对应的 WorkProcessor 去处理
        for (int i = 0; i < numWorkers; i++)
        {
            workProcessors[i] = new WorkProcessor<>(
                    ringBuffer,
                    sequenceBarrier,
                    workHandlers[i],
                    workSequence
            );
        }
    }

    // =============================================================================

    /**
     * 返回 workerPool + workerEventProcessor 的序号数组
     */
    public Sequence[] getWorkerSequences()
    {
        final Sequence[] sequences = new Sequence[workProcessors.length + 1];
        for (int i = 0, size = workProcessors.length; i < size; i++)
        {
            sequences[i] = workProcessors[i].getSequence();
        }
        sequences[sequences.length - 1] = workSequence;

        return sequences;
    }

    /**
     * 启动多线程消费者
     */
    public RingBuffer<T> start(final Executor executor)
    {
        if (!started.compareAndSet(false, true))
        {
            throw new IllegalStateException("WorkerPool 已经被启动, 在被停止前无法再次启动");
        }

        // 生产者序号
        final long cursor = ringBuffer.getCursor().get();

        // 设置 workSequence = cursor
        workSequence.set(cursor);

        // 设置 WorkProcessor.currentConsumeSequence = cursor
        for (WorkProcessor<?> processor : workProcessors)
        {
            processor.getSequence().set(cursor);
            executor.execute(processor);
        }

        return this.ringBuffer;
    }

    public void halt()
    {
        for (WorkProcessor<?> processor : workProcessors)
        {
            // 挨个停止所有工作线程
            processor.halt();
        }

        started.set(false);
    }

    public boolean isRunning()
    {
        return started.get();
    }
}
