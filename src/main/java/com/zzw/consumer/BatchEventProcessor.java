package com.zzw.consumer;

import com.zzw.collection.RingBuffer;
import com.zzw.relation.Sequence;
import com.zzw.relation.SequenceBarrier;
import com.zzw.relation.wait.AlertException;
import com.zzw.util.Util;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 单线程消费者
 */
public class BatchEventProcessor<T> implements EventProcessor
{

    private final RingBuffer<T>           ringBuffer;
    private final EventHandler<? super T> eventHandler;
    private final AtomicBoolean           running = new AtomicBoolean(false);

    // ----------------------------------------

    /**
     * 消费序号
     */
    private final Sequence        sequence = new Sequence(-1);
    /**
     * 消费序号屏障
     */
    private final SequenceBarrier sequenceBarrier;

    // =============================================================================

    public BatchEventProcessor(RingBuffer<T> ringBuffer,
                               SequenceBarrier sequenceBarrier,
                               EventHandler<? super T> eventHandler)
    {
        this.ringBuffer      = ringBuffer;
        this.sequenceBarrier = sequenceBarrier;
        this.eventHandler    = eventHandler;
    }

    @Override
    public Sequence getSequence()
    {
        return sequence;
    }

    // =============================================================================

    @Override
    public void run()
    {
        if (!running.compareAndSet(false, true))
        {
            throw new IllegalStateException("Thread is already running");
        }
        sequenceBarrier.clearAlert();

        // 下一个需要消费的序号
        long nextSequence = sequence.get() + 1;

        // 消费者线程主循环逻辑, 不断的尝试获取事件并进行消费(为了让代码更简单, 暂不考虑优雅停止消费者线程的功能)
        while (true)
        {
            Util.sleep(200); // TODO 为了测试, 让消费者慢一点

            try
            {
                // 可能会抛出 AlertException 异常
                long availableSequence = sequenceBarrier.waitFor(nextSequence);

                while (nextSequence <= availableSequence)
                {
                    // 取出可以消费的下标对应的事件, 交给 eventConsumer 消费
                    T event = ringBuffer.get(nextSequence);
                    eventHandler.onEvent(event, nextSequence, nextSequence == availableSequence);
                    // 批处理, 一次主循环消费 N 个事件(下标加 1, 消费下一个事件)
                    nextSequence++;
                }

                // 更新当前消费者的消费序号
                // set 保证消费者对事件对象的读操作, 一定先于对消费者 Sequence 的更新
                // set 不需要生产者实时的强感知, 这样性能更好, 因为生产者自己也不是实时的读消费者序号的
                sequence.set(availableSequence);
            }
            catch (final AlertException ex)
            {
                // 被外部 alert 打断, 检查 running 标记
                // running == false, break 跳出主循环, 运行结束
                if (!running.get())
                {
                    System.out.println(Thread.currentThread().getName() + " " + eventHandler.getName() + " 退出"); // TODO 为了测试
                    break;
                }
            }
            catch (final Throwable ex)
            {
                // 发生异常, 消费进度依然推进(跳过这一批拉取的数据)
                sequence.set(nextSequence);
                nextSequence++;
            }
        }
    }

    @Override
    public void halt()
    {
        running.set(false);
        sequenceBarrier.alert(); // 唤醒消费者线程(令其能立即检查到状态为停止)
    }

    @Override
    public boolean isRunning()
    {
        return running.get();
    }
}
