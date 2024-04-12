package com.zzw.collection.dsl;

import com.zzw.collection.EventFactory;
import com.zzw.collection.RingBuffer;
import com.zzw.collection.dsl.consumer.ConsumerInfo;
import com.zzw.collection.dsl.consumer.ConsumerRepository;
import com.zzw.collection.dsl.producer.ProducerType;
import com.zzw.consumer.BatchEventProcessor;
import com.zzw.consumer.EventHandler;
import com.zzw.consumer.WorkHandler;
import com.zzw.consumer.WorkerPool;
import com.zzw.relation.Sequence;
import com.zzw.relation.SequenceBarrier;
import com.zzw.relation.wait.WaitStrategy;

import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * disruptor dsl
 */
public class Disruptor<T> {

    /**
     * 环形队列
     */
    private final RingBuffer<T> ringBuffer;
    /**
     * 所有消费者信息的仓库
     */
    private final ConsumerRepository<T> consumerRepository = new ConsumerRepository<>();

    // ----------------------------------------

    /**
     * 执行器
     */
    private final Executor executor;
    /**
     * Disruptor 是否启动
     */
    private final AtomicBoolean started = new AtomicBoolean(false);

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
            final WaitStrategy waitStrategy) {
        this.executor = executor;
        this.ringBuffer = RingBuffer.create(producerType, eventFactory, bufferSize, waitStrategy);
    }

    // =============================================================================

    /**
     * 获得当前 Disruptor 的 ringBuffer
     */
    public RingBuffer<T> getRingBuffer() {
        return ringBuffer;
    }

    /**
     * 启动所有已注册的消费者
     */
    public void start() {
        // CAS 设置启动标识, 避免重复启动
        if (!started.compareAndSet(false, true)) {
            throw new RuntimeException("Disruptor 只能启动一次");
        }

        // 遍历所有的消费者, 挨个 start 启动
        for (ConsumerInfo consumerInfo : consumerRepository.getConsumerInfos()) {
            consumerInfo.start(executor);
        }
    }

    // =============================================================================

    /**
     * 注册单线程消费者(依赖生产序号 + 无上游依赖)
     *
     * @param EventHandlers 用户自定义的事件处理器集合
     */
    @SafeVarargs
    public final EventHandlerGroup<T> handleEventsWith(final EventHandler<T>... EventHandlers) {
        return createEventProcessors(new Sequence[0], EventHandlers);
    }

    /**
     * 注册单线程消费者(依赖生产序号 + 有上游依赖)
     *
     * @param barrierSequences 依赖的上游消费序号集合
     * @param EventHandlers    用户自定义的事件处理器集合
     */
    EventHandlerGroup<T> createEventProcessors(final Sequence[] barrierSequences, final EventHandler<T>[] EventHandlers) {
        final SequenceBarrier barrier = ringBuffer.newBarrier(barrierSequences);  // 序号屏障
        final Sequence[] processorSequences = new Sequence[EventHandlers.length]; // 消费序号

        int i = 0;
        for (EventHandler<T> EventConsumer : EventHandlers) {
            // 创建单线程消费者
            final BatchEventProcessor<T> batchEventProcessor = new BatchEventProcessor<>(ringBuffer, EventConsumer, barrier);

            // 记录消费者的消费序号
            processorSequences[i] = batchEventProcessor.getCurrentConsumeSequence();
            i++;

            // 将消费者加入仓库
            consumerRepository.add(batchEventProcessor);
        }

        // 更新当前生产者注册的消费序号
        updateGatingSequencesForNextInChain(barrierSequences, processorSequences);
        return new EventHandlerGroup<>(this, consumerRepository, processorSequences);
    }

    // ----------------------------------------

    /**
     * 注册多线程消费者(依赖生产序号 + 无上游依赖)
     *
     * @param workHandlers 用户自定义的事件处理器集合
     */
    @SafeVarargs
    public final EventHandlerGroup<T> handleEventsWithWorkerPool(final WorkHandler<T>... workHandlers) {
        return createWorkerPool(new Sequence[0], workHandlers);
    }

    /**
     * 注册多线程消费者 (有上游依赖消费者, 仅依赖生产者序列)
     *
     * @param barrierSequences 依赖的上游消费序号集合
     * @param workHandlers     用户自定义的事件处理器集合
     */
    EventHandlerGroup<T> createWorkerPool(final Sequence[] barrierSequences, final WorkHandler<T>[] workHandlers) {
        // 序号屏障
        final SequenceBarrier sequenceBarrier = ringBuffer.newBarrier(barrierSequences);

        // 创建多线程消费者池
        final WorkerPool<T> workerPool = new WorkerPool<>(ringBuffer, sequenceBarrier, workHandlers);
        // 将消费者池加入仓库
        consumerRepository.add(workerPool);

        // 消费者池的消费序号
        final Sequence[] workerSequences = workerPool.getCurrentWorkerSequences();

        // 更新当前生产者注册的消费序号
        updateGatingSequencesForNextInChain(barrierSequences, workerSequences);
        return new EventHandlerGroup<>(this, consumerRepository, workerSequences);
    }

    // =============================================================================

    /**
     * 更新当前生产者注册的消费序号
     *
     * @param barrierSequences   依赖的上游消费序号集合
     * @param processorSequences 当前消费者的消费序号集合
     */
    private void updateGatingSequencesForNextInChain(final Sequence[] barrierSequences, final Sequence[] processorSequences) {
        // 这是一个优化操作
        // 只需要监控 "消费者链条最末端的序号", 因为它们是最慢的消费序号
        // 不需要监控 "依赖的上游消费序号集合", 这样能使生产者更快的遍历监听的消费序号
        if (processorSequences.length > 0) {
            // 从生产者监控的消费序号中删除 barrierSequences
            for (Sequence sequence : barrierSequences) {
                ringBuffer.removeGatingConsumerSequence(sequence);
            }

            // 向生产者添加多个需要监控的消费序号 processorSequences
            ringBuffer.addGatingConsumerSequenceList(processorSequences);
        }
    }
}
