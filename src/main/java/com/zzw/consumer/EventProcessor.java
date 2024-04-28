package com.zzw.consumer;

import com.zzw.relation.Sequence;

/**
 * 事件处理器接口(消费者)
 */
public interface EventProcessor extends Runnable
{

    /**
     * 获得消费者的消费序号
     *
     * @return 消费序号
     */
    Sequence getCurrentConsumeSequence();

    /**
     * 停止当前消费者线程
     */
    void halt();

    boolean isRunning();
}
