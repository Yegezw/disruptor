package com.zzw.collection.dsl;

import com.zzw.collection.EventFactory;
import com.zzw.collection.RingBuffer;
import com.zzw.collection.dsl.consumer.ConsumerInfo;
import com.zzw.collection.dsl.consumer.ConsumerRepository;
import com.zzw.collection.dsl.producer.ProducerType;
import com.zzw.collection.exception.ExceptionHandler;
import com.zzw.consumer.BatchEventProcessor;
import com.zzw.consumer.EventHandler;
import com.zzw.consumer.WorkHandler;
import com.zzw.consumer.WorkerPool;
import com.zzw.relation.Sequence;
import com.zzw.relation.SequenceBarrier;
import com.zzw.relation.wait.WaitStrategy;

import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * disruptor dsl
 */
public class Disruptor<T>
{

    /**
     * 环形队列
     */
    private final RingBuffer<T>         ringBuffer;
    /**
     * 所有消费者信息的仓库
     */
    private final ConsumerRepository<T> consumerRepository = new ConsumerRepository<>();

    // ----------------------------------------

    /**
     * 执行器
     */
    private final Executor      executor;
    /**
     * Disruptor 是否启动
     */
    private final AtomicBoolean started = new AtomicBoolean(false);

    // ----------------------------------------

    /**
     * 异常处理器
     */
    private ExceptionHandler<? super T> exceptionHandler = new ExceptionHandlerWrapper<>();

    // =============================================================================

    /**
     * 创建 Disruptor
     *
     * @param eventFactory 用户自定义的事件工厂
     * @param bufferSize   ringBuffer 容量
     * @param executor     执行器
     * @param producerType 生产者类型(单线程生产者 OR 多线程生产者)
     * @param waitStrategy 指定的消费者阻塞策略
     */
    public Disruptor(
            final EventFactory<T> eventFactory,
            final int bufferSize,
            final Executor executor,
            final ProducerType producerType,
            final WaitStrategy waitStrategy)
    {
        this.executor   = executor;
        this.ringBuffer = RingBuffer.create(producerType, eventFactory, bufferSize, waitStrategy);
    }

    public void handleExceptionsWith(final ExceptionHandler<? super T> exceptionHandler)
    {
        this.exceptionHandler = exceptionHandler;
    }

    @SuppressWarnings("unchecked")
    public void setDefaultExceptionHandler(final ExceptionHandler<? super T> exceptionHandler)
    {
        checkNotStarted();
        if (!(this.exceptionHandler instanceof ExceptionHandlerWrapper))
        {
            throw new IllegalStateException("setDefaultExceptionHandler can not be used after handleExceptionsWith");
        }
        ((ExceptionHandlerWrapper<T>) this.exceptionHandler).switchTo(exceptionHandler);
    }

    // =============================================================================

    /**
     * 获得当前 Disruptor 的 ringBuffer
     */
    public RingBuffer<T> getRingBuffer()
    {
        return ringBuffer;
    }

    /**
     * 启动所有已注册的消费者
     */
    public void start()
    {
        // CAS 设置启动标识, 避免重复启动
        if (!started.compareAndSet(false, true))
        {
            throw new RuntimeException("Disruptor 只能启动一次");
        }

        // 遍历所有的消费者, 挨个 start 启动
        for (ConsumerInfo consumerInfo : consumerRepository.getConsumerInfos())
        {
            consumerInfo.start(executor);
        }
    }

    private void checkNotStarted()
    {
        if (started.get())
        {
            throw new IllegalStateException("All event handlers must be added before calling starts.");
        }
    }

    // =============================================================================

    /**
     * 注册单线程消费者(依赖生产序号 + 无上游依赖)
     *
     * @param eventHandlers 用户自定义的事件处理器数组
     */
    @SafeVarargs
    public final EventHandlerGroup<T> handleEventsWith(final EventHandler<? super T>... eventHandlers)
    {
        return createEventProcessors(new Sequence[0], eventHandlers);
    }

    /**
     * 注册单线程消费者(依赖生产序号 + 有上游依赖)
     *
     * @param barrierSequences 依赖的上游消费序号数组
     * @param eventHandlers    用户自定义的事件处理器数组
     */
    EventHandlerGroup<T> createEventProcessors(final Sequence[] barrierSequences,
                                               final EventHandler<? super T>[] eventHandlers)
    {
        checkNotStarted();

        final SequenceBarrier barrier            = ringBuffer.newBarrier(barrierSequences); // 序号屏障
        final Sequence[]      processorSequences = new Sequence[eventHandlers.length];      // 消费序号

        for (int i = 0, eventHandlersLength = eventHandlers.length; i < eventHandlersLength; i++)
        {
            final EventHandler<? super T> eventHandler = eventHandlers[i];

            // 创建单线程消费者
            final BatchEventProcessor<T> batchEventProcessor =
                    new BatchEventProcessor<>(ringBuffer, barrier, eventHandler);
            if (exceptionHandler != null)
            {
                batchEventProcessor.setExceptionHandler(exceptionHandler);
            }

            consumerRepository.add(batchEventProcessor);               // 将消费者加入仓库
            processorSequences[i] = batchEventProcessor.getSequence(); // 记录消费者的消费序号
        }

        // 更新当前生产者注册的消费序号
        updateGatingSequencesForNextInChain(barrierSequences, processorSequences);
        return new EventHandlerGroup<>(this, consumerRepository, processorSequences);
    }

    // ----------------------------------------

    /**
     * 注册多线程消费者(依赖生产序号 + 无上游依赖)
     *
     * @param workHandlers 用户自定义的事件处理器数组
     */
    @SafeVarargs
    public final EventHandlerGroup<T> handleEventsWithWorkerPool(final WorkHandler<T>... workHandlers)
    {
        return createWorkerPool(new Sequence[0], workHandlers);
    }

    /**
     * 注册多线程消费者 (有上游依赖消费者, 仅依赖生产者序列)
     *
     * @param barrierSequences 依赖的上游消费序号数组
     * @param workHandlers     用户自定义的事件处理器数组
     */
    EventHandlerGroup<T> createWorkerPool(final Sequence[] barrierSequences,
                                          final WorkHandler<? super T>[] workHandlers)
    {
        // 序号屏障
        final SequenceBarrier sequenceBarrier = ringBuffer.newBarrier(barrierSequences);

        // 创建多线程消费者池
        final WorkerPool<T> workerPool = new WorkerPool<>(ringBuffer, sequenceBarrier, exceptionHandler, workHandlers);
        // 将消费者池加入仓库
        consumerRepository.add(workerPool);

        // 消费者池的消费序号
        final Sequence[] workerSequences = workerPool.getWorkerSequences();

        // 更新当前生产者注册的消费序号
        updateGatingSequencesForNextInChain(barrierSequences, workerSequences);
        return new EventHandlerGroup<>(this, consumerRepository, workerSequences);
    }

    // ----------------------------------------

    /**
     * 更新当前生产者注册的消费序号
     *
     * @param barrierSequences   依赖的上游消费序号数组
     * @param processorSequences 当前消费者的消费序号数组
     */
    private void updateGatingSequencesForNextInChain(final Sequence[] barrierSequences,
                                                     final Sequence[] processorSequences)
    {
        // 这是一个优化操作
        // 只需要监控 "消费者链条最末端的序号", 因为它们是最慢的消费序号
        // 不需要监控 "依赖的上游消费序号数组", 这样能使生产者更快的遍历监听的消费序号
        if (processorSequences.length > 0)
        {
            // 从生产者监控的消费序号中删除 barrierSequences
            for (final Sequence sequence : barrierSequences)
            {
                ringBuffer.removeGatingSequence(sequence);
            }

            // 向生产者添加多个需要监控的消费序号 processorSequences
            ringBuffer.addGatingSequences(processorSequences);

            // 将 barrierSequences 所属的 ConsumerInfo.endOfChain 标记为 "非最尾端的消费者"(用于 shutdown 优雅停止)
            consumerRepository.unMarkEventProcessorsAsEndOfChain(barrierSequences);
        }
    }

    // =============================================================================

    /**
     * 停止所有消费者线程
     */
    public void halt()
    {
        for (final ConsumerInfo consumerInfo : consumerRepository.getConsumerInfos())
        {
            consumerInfo.halt();
        }
    }

    /**
     * 等所有消费者线程 "把已生产的事件全部消费完成" 后, 再停止所有消费者线程
     */
    public void shutdown()
    {
        try
        {
            shutdown(-1, TimeUnit.MICROSECONDS);
        }
        catch (final TimeoutException e)
        {
            exceptionHandler.handleOnShutdownException(e);
        }
    }

    /**
     * 等所有消费者线程 "把已生产的事件全部消费完成" 后, 再停止所有消费者线程
     */
    public void shutdown(long timeout, TimeUnit timeUnit) throws TimeoutException
    {
        final long timeOutAt = System.currentTimeMillis() + timeUnit.toMillis(timeout);

        while (hasBacklog())
        {
            if (timeout >= 0 && System.currentTimeMillis() > timeOutAt)
            {
                throw TimeoutException.INSTANCE;
            }
            // Busy spin
        }

        // hasBacklog 为 false, 跳出了循环
        // 说明已生产的事件全部消费完成了, 此时可以安全的优雅停止所有消费者线程了
        halt();
    }

    /**
     * 判断最尾端消费者线程 "是否还有未消费完的事件"
     */
    private boolean hasBacklog()
    {
        final long cursor = ringBuffer.getCursor().get();

        // 获取所有处于最尾端的消费者序号(最尾端的是最慢的, 所以是准确的)
        for (final Sequence consumer : consumerRepository.getLastSequenceInChain())
        {
            if (cursor > consumer.get())
            {
                // 如果任意一个消费序号 < 当前生产序号, 说明存在未消费完的事件, 返回 true
                return true;
            }
        }

        // 所有 "最尾端消费者线程的序号" >= "生产序号"
        // 说明所有消费者线程都已经 "把已生产的事件全部消费完成", 返回 false
        return false;
    }
}
