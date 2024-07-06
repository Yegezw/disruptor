package com.zzw.producer;

/**
 * 事件更新器
 * @param <T> event 类型
 */
public interface EventTranslatorVararg<T>
{

    /**
     * 将 args 更新到 event 中
     *
     * @param event    事件对象
     * @param sequence 事件对象在 RingBuffer 里的序号
     * @param args     更新事件对象 event 所需要的参数
     */
    void translateTo(T event, long sequence, Object... args);
}
