package com.zzw.relation;

import com.zzw.producer.Sequencer;
import com.zzw.relation.wait.WaitStrategy;

import java.util.Collections;
import java.util.List;

/**
 * 消费序号屏障
 */
public class SequenceBarrier {

    /**
     * "单线程 OR 多线程" 生产者序号生成器
     */
    private final Sequencer producerSequencer;
    /**
     * <p>生产序号
     * <p>单线程下表示: 当前生产序号(已发布)
     * <p>多线程下表示: 多线程共同的已申请序号(可能未发布)
     */
    private final Sequence currentProducerSequence;
    /**
     * 当前消费者所依赖的上游消费者序号集合
     */
    private final List<Sequence> dependentSequencesList;
    private final WaitStrategy waitStrategy;

    public SequenceBarrier(Sequencer producerSequencer,
                           Sequence currentProducerSequence,
                           WaitStrategy waitStrategy,
                           List<Sequence> dependentSequencesList) {
        this.producerSequencer = producerSequencer;
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
        long availableSequence = waitStrategy.waitFor(currentConsumeSequence, currentProducerSequence, dependentSequencesList);

        if (availableSequence < currentConsumeSequence) {
            return availableSequence;
        }

        // 多线程生产者中, 需要进一步约束(于 v4 版本新增)
        return producerSequencer.getHighestPublishedSequence(currentConsumeSequence, availableSequence);
    }
}
