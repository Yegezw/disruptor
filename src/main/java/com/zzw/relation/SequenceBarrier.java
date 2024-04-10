package com.zzw.relation;

import com.zzw.relation.wait.WaitStrategy;

import java.util.Collections;
import java.util.List;

/**
 * 消费序号屏障
 */
public class SequenceBarrier {

    /**
     * 生产序号
     */
    private final Sequence currentProducerSequence;
    private final WaitStrategy waitStrategy;
    /**
     * 当前消费者所依赖的上游消费者序号集合
     */
    private final List<Sequence> dependentSequencesList;

    public SequenceBarrier(Sequence currentProducerSequence, WaitStrategy waitStrategy, List<Sequence> dependentSequencesList) {
        this.currentProducerSequence = currentProducerSequence;
        this.waitStrategy = waitStrategy;
        if (!dependentSequencesList.isEmpty()) {
            this.dependentSequencesList = dependentSequencesList;
        } else {
            // 如果传入的上游消费者序号集合为空, 就用生产者序号作为兜底的依赖
            this.dependentSequencesList = Collections.singletonList(currentProducerSequence);
        }
    }

    /**
     * 等待给定的序号可供使用
     *
     * @param currentConsumeSequence 下一个需要消费的序号
     * @return 最大可消费序号
     */
    public long getAvailableConsumeSequence(long currentConsumeSequence) throws InterruptedException {
        // v1 版本只是简单的调用 waitFor, 等待其返回即可
        return this.waitStrategy.waitFor(currentConsumeSequence, currentProducerSequence, dependentSequencesList);
    }
}
