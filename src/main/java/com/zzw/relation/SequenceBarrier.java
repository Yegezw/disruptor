package com.zzw.relation;

import com.zzw.relation.wait.WaitStrategy;

/**
 * 消费序号屏障
 */
public class SequenceBarrier {

    /**
     * 生产序号
     */
    private final Sequence currentProducerSequence;
    private final WaitStrategy waitStrategy;

    public SequenceBarrier(Sequence currentProducerSequence, WaitStrategy waitStrategy) {
        this.currentProducerSequence = currentProducerSequence;
        this.waitStrategy = waitStrategy;
    }

    /**
     * 等待给定的序号可供使用
     *
     * @param currentConsumeSequence 下一个需要消费的序号
     * @return 最大可消费序号
     */
    public long getAvailableConsumeSequence(long currentConsumeSequence) throws InterruptedException {
        // v1 版本只是简单的调用 waitFor, 等待其返回即可
        return this.waitStrategy.waitFor(currentConsumeSequence, currentProducerSequence);
    }
}
