package com.zzw.core.producer;

/**
 * 事件工厂
 */
public interface EventFactory<E>
{

    /**
     * 创建一个裸事件, 用于填充 RingBuffer
     */
    E newInstance();
}
