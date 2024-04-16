package com.zzw.p3_consumer;

/**
 * 事件处理器
 */
public interface EventHandler<E>
{

    /**
     * @see BatchEventProcessor#processEvents()
     */
    void onEvent(E event, long Sequence, boolean endOfBatch);
}
