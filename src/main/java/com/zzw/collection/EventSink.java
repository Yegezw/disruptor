package com.zzw.collection;

import com.zzw.producer.EventTranslatorVararg;

/**
 * 事件接收器
 */
public interface EventSink<E>
{

    /**
     * 发布一个事件
     *
     * @param translator 事件更新器
     * @param args       更新事件对象 event 所需要的参数
     */
    void publishEvent(EventTranslatorVararg<E> translator, Object... args);

    /**
     * 发布一批事件
     *
     * @param translator 事件更新器
     * @param args       更新事件对象 event 所需要的参数, [发布的事件个数][事件参数]
     */
    void publishEvents(EventTranslatorVararg<E> translator, Object[]... args);

    /**
     * 发布一批事件 args[batchStartsAt ... batchStartsAt + batchSize - 1]
     *
     * @param translator    事件更新器
     * @param batchStartsAt 发布的事件起始数
     * @param batchSize     发布的事件个数
     * @param args          更新事件对象 event 所需要的参数, [事件总个数][事件参数]
     */
    void publishEvents(EventTranslatorVararg<E> translator, int batchStartsAt, int batchSize, Object[]... args);
}
