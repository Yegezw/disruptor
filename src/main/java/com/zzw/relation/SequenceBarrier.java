package com.zzw.relation;

import com.zzw.producer.Sequencer;
import com.zzw.relation.wait.AlertException;
import com.zzw.relation.wait.WaitStrategy;

/**
 * 消费序号屏障
 */
public class SequenceBarrier
{

    /**
     * "单线程 OR 多线程" 生产者序号生成器
     */
    private final    Sequencer    producerSequencer;
    /**
     * <p>生产序号
     * <p>单线程下表示: 当前生产序号(已发布)
     * <p>多线程下表示: 多线程共同的已申请序号(可能未发布)
     */
    private final    Sequence     currentProducerSequence;
    /**
     * 当前消费者所依赖的上游消费者序号数组
     */
    private final    Sequence[]   dependentSequences;
    private final    WaitStrategy waitStrategy;
    private volatile boolean      alerted = false;

    public SequenceBarrier(Sequencer producerSequencer,
                           Sequence currentProducerSequence,
                           WaitStrategy waitStrategy,
                           Sequence[] dependentSequences)
    {
        this.producerSequencer       = producerSequencer;
        this.currentProducerSequence = currentProducerSequence;
        this.waitStrategy            = waitStrategy;
        if (dependentSequences.length != 0)
        {
            this.dependentSequences = dependentSequences;
        }
        else
        {
            // 如果传入的上游消费者序号数组为空, 就用生产者序号作为兜底的依赖
            this.dependentSequences = new Sequence[]{currentProducerSequence};
        }
    }

    // =============================================================================

    /**
     * 等待给定的序号可供使用
     *
     * @param currentConsumeSequence 下一个需要消费的序号
     * @return 最大可消费序号
     */
    public long getAvailableConsumeSequence(long currentConsumeSequence) throws InterruptedException, AlertException
    {
        checkAlert(); // 每次都检查下是否被唤醒, 被唤醒则会抛出 AlertException, 代表当前的消费者线程要终止运行了

        long availableSequence = waitStrategy.waitFor(currentConsumeSequence, currentProducerSequence, dependentSequences, this);

        if (availableSequence < currentConsumeSequence)
        {
            return availableSequence;
        }

        // 多线程生产者中, 需要进一步约束(于 v4 版本新增)
        return producerSequencer.getHighestPublishedSequence(currentConsumeSequence, availableSequence);
    }

    // =============================================================================

    /**
     * 唤醒阻塞在 waitStrategy 的消费者线程
     */
    public void alert()
    {
        alerted = true;
        waitStrategy.signalWhenBlocking();
    }

    /**
     * 重新启动时清除被唤醒标记
     */
    public void clearAlert()
    {
        alerted = false;
    }

    /**
     * 检查是否被唤醒, 如果被唤醒则抛出 AlertException 异常
     */
    public void checkAlert() throws AlertException
    {
        if (alerted)
        {
            throw AlertException.INSTANCE;
        }
    }
}
