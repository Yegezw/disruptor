package com.zzw.collection;

/**
 * 事件工厂
 */
public interface EventFactory<T>
{

    /**
     * 创建一个裸事件, 用于填充 RingBuffer
     */
    T newInstance();
}
