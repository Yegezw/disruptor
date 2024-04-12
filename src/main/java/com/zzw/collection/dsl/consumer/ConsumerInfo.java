package com.zzw.collection.dsl.consumer;

import java.util.concurrent.Executor;

/**
 * 消费者信息
 */
public interface ConsumerInfo {

    /**
     * 通过 executor 启动当前消费者线程
     * @param executor 执行器
     */
    void start(Executor executor);
}
