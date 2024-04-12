package com.zzw.consumer;

import com.zzw.collection.RingBuffer;
import com.zzw.relation.Sequence;
import com.zzw.relation.SequenceBarrier;
import com.zzw.util.Util;

/**
 * 单线程消费者
 */
public class BatchEventProcessor<T> implements EventProcessor {

    private final RingBuffer<T> ringBuffer;
    private final EventHandler<T> eventConsumer;

    // ----------------------------------------

    /**
     * 消费序号
     */
    private final Sequence currentConsumeSequence = new Sequence(-1);
    /**
     * 消费序号屏障
     */
    private final SequenceBarrier sequenceBarrier;

    // =============================================================================

    public BatchEventProcessor(RingBuffer<T> ringBuffer,
                               EventHandler<T> eventConsumer,
                               SequenceBarrier sequenceBarrier) {
        this.ringBuffer = ringBuffer;
        this.eventConsumer = eventConsumer;
        this.sequenceBarrier = sequenceBarrier;
    }

    @Override
    public Sequence getCurrentConsumeSequence() {
        return currentConsumeSequence;
    }

    // =============================================================================

    @Override
    public void run() {
        // 下一个需要消费的序号
        long nextConsumerIndex = currentConsumeSequence.get() + 1;

        // 消费者线程主循环逻辑, 不断的尝试获取事件并进行消费(为了让代码更简单, 暂不考虑优雅停止消费者线程的功能)
        while (true) {
            Util.sleep(200); // 为了测试, 让消费者慢一点

            try {
                long availableConsumeIndex = this.sequenceBarrier.getAvailableConsumeSequence(nextConsumerIndex);

                while (nextConsumerIndex <= availableConsumeIndex) {
                    // 取出可以消费的下标对应的事件, 交给 eventConsumer 消费
                    T event = ringBuffer.get(nextConsumerIndex);
                    this.eventConsumer.consume(event, nextConsumerIndex, nextConsumerIndex == availableConsumeIndex);
                    // 批处理, 一次主循环消费 N 个事件(下标加 1, 消费下一个事件)
                    nextConsumerIndex++;
                }

                // 更新当前消费者的消费序号
                // lazySet 保证消费者对事件对象的读操作, 一定先于对消费者 Sequence 的更新
                // lazySet 不需要生产者实时的强感知, 这样性能更好, 因为生产者自己也不是实时的读消费者序号的
                this.currentConsumeSequence.lazySet(availableConsumeIndex);
            } catch (final Throwable ex) {
                // 发生异常, 消费进度依然推进(跳过这一批拉取的数据)
                this.currentConsumeSequence.lazySet(nextConsumerIndex);
                nextConsumerIndex++;
            }
        }
    }
}
