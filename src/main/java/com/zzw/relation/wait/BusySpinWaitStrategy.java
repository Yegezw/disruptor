package com.zzw.relation.wait;

import com.zzw.relation.Sequence;
import com.zzw.util.SequenceUtil;

import java.util.List;

/**
 * 自旋等待策略
 */
public class BusySpinWaitStrategy implements WaitStrategy {

    /**
     * <p>等待给定的序号可供使用, 由消费者调用
     *
     * @param currentConsumeSequence  下一个需要消费的序号
     * @param currentProducerSequence 生产序号
     * @param dependentSequenceList   当前消费者所依赖的上游消费者序号集合
     * @return 最大可消费序号
     */
    @Override
    public long waitFor(long currentConsumeSequence, Sequence currentProducerSequence, List<Sequence> dependentSequenceList) throws InterruptedException {
        long availableSequence;

        while ((availableSequence = SequenceUtil.getMinimumSequence(dependentSequenceList)) < currentConsumeSequence) {
            // 由于消费者消费速度一般会很快, 所以这里使用自旋阻塞来等待上游消费者进度推进(响应及时且实现简单)
            // 在 JDK 9 开始引入的 Thread.onSpinWait 方法, 优化自旋性能
            ThreadHints.onSpinWait();
        }

        return availableSequence;
    }

    @Override
    public void signalWhenBlocking() {
    }
}