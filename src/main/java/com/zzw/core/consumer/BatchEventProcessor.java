package com.zzw.core.consumer;

import com.zzw.core.consumer.barrier.SequenceBarrier;
import com.zzw.core.RingBuffer;
import com.zzw.core.Sequence;

/**
 * 批事件处理线程
 */
public class BatchEventProcessor<E> implements Runnable
{

    /**
     * 事件容器
     */
    private final RingBuffer<E>   ringBuffer;
    /**
     * 事件处理器
     */
    private final EventHandler<E> eventHandler;

    // ----------------------------------------

    /**
     * 消费序号, 代表已消费
     */
    private final Sequence        sequence = new Sequence(Sequence.INITIAL_VALUE);
    /**
     * 序号屏障
     */
    private final SequenceBarrier barrier;

    // =============================================================================
    public BatchEventProcessor(RingBuffer<E> ringBuffer, EventHandler<E> eventHandler, SequenceBarrier barrier)
    {
        this.ringBuffer   = ringBuffer;
        this.eventHandler = eventHandler;
        this.barrier      = barrier;
    }

    public Sequence getSequence()
    {
        return sequence;
    }

    // =============================================================================

    @Override
    public void run()
    {
        processEvents();
    }

    /**
     * @see SequenceBarrier#waitFor(long)
     * @see EventHandler#onEvent(Object, long, boolean)
     */
    private void processEvents()
    {
        E    event        = null;                // 事件
        long nextSequence = sequence.get() + 1L; // 申请消费的序号

        while (true)
        {
            try
            {
                // 获取最大可消费序号
                long availableSequence = barrier.waitFor(nextSequence);

                // 消费一批
                while (nextSequence <= availableSequence)
                {
                    event = ringBuffer.get(nextSequence);
                    eventHandler.onEvent(event, nextSequence, nextSequence == availableSequence);
                    nextSequence++;
                }

                // 保证写操作的有序性
                // Event 的读取 -> Sequence 的更新
                sequence.set(availableSequence);
            } catch (Throwable ex)
            {
                // 发生异常, 消费序号依然推进
                sequence.set(nextSequence);
                nextSequence++;
            }
        }
    }
}
