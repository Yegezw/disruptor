package com.zzw.collection.dsl;

import com.zzw.collection.dsl.consumer.ConsumerRepository;
import com.zzw.consumer.EventHandler;
import com.zzw.consumer.WorkHandler;
import com.zzw.relation.Sequence;

/**
 * 事件处理器组
 */
public class EventHandlerGroup<T>
{

    private final Disruptor<T>          disruptor;
    /**
     * disruptor 所有消费者信息的仓库
     */
    private final ConsumerRepository<T> consumerRepository;
    /**
     * 当前事件处理器组内的所有消费者的消费序号
     */
    private final Sequence[]            sequences;

    // =============================================================================

    public EventHandlerGroup(Disruptor<T> disruptor,
                             ConsumerRepository<T> consumerRepository,
                             Sequence[] sequences)
    {
        this.disruptor          = disruptor;
        this.consumerRepository = consumerRepository;
        this.sequences          = sequences;
    }

    // =============================================================================

    @SafeVarargs
    public final EventHandlerGroup<T> then(final EventHandler<? super T>... handlers)
    {
        return handleEventsWith(handlers);
    }

    /**
     * 向 disruptor 注册单线程消费者(依赖生产序号 + 上游依赖为 sequences)
     */
    @SafeVarargs
    public final EventHandlerGroup<T> handleEventsWith(final EventHandler<? super T>... handlers)
    {
        return disruptor.createEventProcessors(sequences, handlers);
    }

    // ----------------------------------------

    @SafeVarargs
    public final EventHandlerGroup<T> thenHandleEventsWithWorkerPool(final WorkHandler<? super T>... handlers)
    {
        return handleEventsWithWorkerPool(handlers);
    }

    /**
     * 向 disruptor 注册多线程消费者(依赖生产序号 + 上游依赖为 sequences)
     */
    @SafeVarargs
    public final EventHandlerGroup<T> handleEventsWithWorkerPool(final WorkHandler<? super T>... handlers)
    {
        return disruptor.createWorkerPool(sequences, handlers);
    }
}
