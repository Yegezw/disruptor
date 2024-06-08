package com.zzw.producer;

import com.zzw.relation.Sequence;
import com.zzw.relation.SequenceBarrier;

/**
 * 序号生成器
 */
public interface Sequencer
{

    /**
     * 获取 RingBuffer 的大小
     *
     * @return RingBuffer 的大小
     */
    int getBufferSize();

    /**
     * 获取当前生产序号
     *
     * @return 当前生产序号
     */
    Sequence getCursor();

    // ----------------------------------------

    /**
     * 获取一个仅依赖生产序号的序号屏障
     *
     * @return 仅依赖生产序号的序号屏障
     */
    SequenceBarrier newBarrier();

    /**
     * 获取一个依赖 "生产序号 + 上游消费序号" 的序号屏障
     *
     * @param sequencesToTrack 依赖的上游消费序号数组
     * @return 依赖 "生产序号 + 上游消费序号" 的序号屏障
     */
    SequenceBarrier newBarrier(Sequence... sequencesToTrack);

    /**
     * 向生产者添加多个需要监控的消费序号
     *
     * @param gatingSequences 需要监控的消费序号数组
     */
    void addGatingSequences(Sequence... gatingSequences);

    /**
     * 从生产者监控的消费序号中 "移除目标消费序号"
     *
     * @param sequence 待移除的目标消费序号
     */
    void removeGatingSequence(Sequence sequence);

    // ----------------------------------------

    /**
     * 获取一个生产序号
     *
     * @return 生产序号
     */
    long next();

    /**
     * 获取 n 个生产序号
     *
     * @param n 生产序号个数
     * @return 最大生产序号
     */
    long next(int n);

    /**
     * 发布一个生产序号
     *
     * @param publishIndex 需要发布的生产序号
     */
    void publish(long publishIndex);

    // ----------------------------------------

    /**
     * 获取 "连续的 + 已发布的 + 最大的" 生产序号
     *
     * @param lowerBound        下一个需要消费的序号
     * @param availableSequence 最大可消费序号
     * @return "连续的 + 已发布的 + 最大的" 生产序号
     */
    long getHighestPublishedSequence(long lowerBound, long availableSequence);
}
