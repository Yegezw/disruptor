package com.zzw.consumer;

import com.zzw.collection.RingBuffer;
import com.zzw.relation.Sequence;
import com.zzw.relation.SequenceBarrier;

/**
 * 多线程消费者的工作线程
 */
public class WorkProcessor<T> implements EventProcessor {

    private final RingBuffer<T> ringBuffer;
    private final WorkHandler<T> workHandler;
    /**
     * 消费序号
     */
    private final Sequence currentConsumeSequence = new Sequence(-1);

    // ----------------------------------------

    /**
     * <p>工作消费序号, 多线程消费者共同的已申请序号(可能未消费)
     * <p>主要用于通过 CAS 协调同一 WorkerPool 内消费者线程争抢序号
     */
    private final Sequence workSequence;
    /**
     * 消费序号屏障
     */
    private final SequenceBarrier sequenceBarrier;

    // =============================================================================

    public WorkProcessor(RingBuffer<T> ringBuffer,
                         WorkHandler<T> workHandler,
                         SequenceBarrier sequenceBarrier,
                         Sequence workSequence) {
        this.ringBuffer = ringBuffer;
        this.workHandler = workHandler;
        this.sequenceBarrier = sequenceBarrier;
        this.workSequence = workSequence;
    }

    @Override
    public Sequence getCurrentConsumeSequence() {
        return currentConsumeSequence;
    }

    // =============================================================================

    @Override
    public void run() {
        // 下一个需要消费的序号
        long nextConsumerIndex = this.currentConsumeSequence.get() + 1;

        // 设置哨兵值
        // 保证第一次循环时, nextConsumerIndex <= cachedAvailableSequence 一定为 false
        // 走 else 分支, 通过序号屏障获得最大的可用序号号
        long cachedAvailableSequence = Long.MIN_VALUE; // 缓存的最大可消费序号
        boolean processedSequence = true;              // 最近是否处理过了序号

        while (true) {
            try {
                // 如果已经处理过序号, 则重新 CAS 争抢一个新的待消费序号
                if (processedSequence) {
                    // 争抢到了一个新的待消费序号, 但还未实际进行消费(标记为 false)
                    processedSequence = false;

                    // 如果已经处理过序号, 则重新 CAS 的争抢一个新的待消费序号
                    do {
                        nextConsumerIndex = this.workSequence.get() + 1L;

                        // 由于 currentConsumeSequence 会被注册到生产者
                        // 因此需要始终和 workSequence 保持协调
                        // 即当前 currentConsumeSequence = workSequence
                        this.currentConsumeSequence.lazySet(nextConsumerIndex - 1L);

                        // 问题: 只使用 workSequence, 每个 worker 不维护 currentConsumeSequence 行不行
                        // 回答: 这是不行的
                        // 因为和单线程消费者的行为一样
                        // 都是具体的消费者 eventHandler/workHandler 执行过之后, 才更新消费者序号, 令其对外部可见(生产者、下游消费者)
                        // 因为消费依赖关系中约定, 对于序号 i 事件
                        // 只有在上游的消费者消费过后(eventHandler/workHandler 执行过), 下游才能消费序号 i 事件
                        // 1、workSequence 主要是用于通过 CAS 协调同一 workerPool 内消费者线程序号争抢的
                        // 2、对外的约束依然需要 workProcessor 本地的消费者序号 currentConsumeSequence 来控制

                        // CAS 更新, 保证每个 worker 线程都会获取到唯一的一个 sequence
                    } while (!workSequence.compareAndSet(nextConsumerIndex - 1L, nextConsumerIndex));
                } else {
                    // processedSequence == false(手头上存在一个还未消费的序号), 走到这里说明
                    // 之前拿到了一个新的消费序号, 但是由于 nextConsumerIndex > cachedAvailableSequence, 没有实际执行消费逻辑
                    // 而是被 sequenceBarrier 阻塞, 醒来后获得了最新的 cachedAvailableSequence, 重新执行循环走到了这里
                    // 需要先把手头上的这个序号给消费掉, 才能继续拿下一个消费序号
                }

                // cachedAvailableSequence 只会存在两种情况
                // 1、第一次循环, 初始化为 Long.MIN_VALUE, 则必定会走到下面的 else 分支中
                // 2、非第一次循环, 则 cachedAvailableSequence 为序号屏障所允许的最大可消费序号
                if (nextConsumerIndex <= cachedAvailableSequence) {
                    // 取出可以消费的下标所对应的事件, 交给 eventConsumer 消费
                    T event = ringBuffer.get(nextConsumerIndex);
                    this.workHandler.consume(event);

                    // 实际调用消费者进行消费了, 标记为 true, 这样一来就可以在下次循环中 CAS 争抢下一个新的消费序号了
                    processedSequence = true;
                } else {
                    // 1、第一次循环, 获取当前序号屏障的最大可消费序号
                    // 2、非第一次循环, 说明争抢到的序号 > "缓存的最大可消费序号", 等待 "生产者/上游消费者" 推进到争抢到的 nextConsumerIndex
                    cachedAvailableSequence = sequenceBarrier.getAvailableConsumeSequence(nextConsumerIndex);
                }
            } catch (final Throwable ex) {
                // 消费者消费时发生了异常, 也认为是成功消费了, 下次循环会 CAS 争抢一个新的消费序号
                processedSequence = true;
            }
        }
    }
}
