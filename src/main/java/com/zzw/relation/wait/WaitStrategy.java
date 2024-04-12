package com.zzw.relation.wait;

import com.zzw.relation.Sequence;
import com.zzw.relation.SequenceBarrier;

import java.util.List;

/**
 * 消费者等待策略
 */
public interface WaitStrategy {

    /**
     * 等待给定的序号可供使用, 由消费者调用
     *
     * @param currentConsumeSequence  下一个需要消费的序号
     * @param currentProducerSequence 生产序号
     * @param dependentSequenceList   当前消费者所依赖的上游消费者序号集合
     * @param barrier                 消费序号屏障
     * @return 最大可消费序号
     */
    long waitFor(long currentConsumeSequence,
                 Sequence currentProducerSequence,
                 List<Sequence> dependentSequenceList,
                 SequenceBarrier barrier) throws InterruptedException, AlertException;

    /**
     * 唤醒 waitFor 阻塞在该等待策略对象上的消费者线程, 由生产者调用
     */
    void signalWhenBlocking();
}
