package com.zzw.relation.wait;

import com.zzw.relation.Sequence;
import com.zzw.relation.SequenceBarrier;
import com.zzw.util.SequenceUtil;

/**
 * 自旋等待策略
 */
public class BusySpinWaitStrategy implements WaitStrategy
{

    /**
     * <p>等待给定的序号可供使用, 由消费者调用
     *
     * @param sequence           下一个需要消费的序号
     * @param cursor             生产序号
     * @param dependentSequences 当前消费者所依赖的上游消费者序号数组
     * @return 最大可消费序号
     */
    @Override
    public long waitFor(long sequence,
                        Sequence cursor,
                        Sequence[] dependentSequences,
                        SequenceBarrier barrier) throws InterruptedException, AlertException
    {
        long availableSequence;

        while ((availableSequence = SequenceUtil.getMinimumSequence(dependentSequences)) < sequence)
        {
            // 每次循环都检查运行状态
            barrier.checkAlert();

            // 由于消费者消费速度一般会很快, 所以这里使用自旋阻塞来等待上游消费者进度推进(响应及时且实现简单)
            // 在 JDK 9 开始引入的 Thread.onSpinWait 方法, 优化自旋性能
            ThreadHints.onSpinWait();
        }

        return availableSequence;
    }

    @Override
    public void signalAllWhenBlocking()
    {
    }
}
