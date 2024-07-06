package com.zzw.collection;

import com.zzw.collection.dsl.producer.ProducerType;
import com.zzw.producer.EventTranslatorVararg;
import com.zzw.producer.MultiProducerSequencer;
import com.zzw.producer.Sequencer;
import com.zzw.producer.SingleProducerSequencer;
import com.zzw.relation.Sequence;
import com.zzw.relation.SequenceBarrier;
import com.zzw.relation.wait.WaitStrategy;

/**
 * 环形队列
 */
public class RingBuffer<E> implements EventSink<E>
{

    /**
     * 避免伪共享, 左半部分填充
     */
    protected long p11, p12, p13, p14, p15, p16, p17;

    // ----------------------------------------

    /**
     * 填充个数: 32 * 4 = 128 bytes
     */
    private static final int BUFFER_PAD = 32;
    /**
     * <p>环形数组
     * <p>数组对象头｜填充的 128 个无效字节｜有效数据(引用)｜填充的 128 个无效字节
     */
    private final        E[] entries;
    private final        int bufferSize;
    private final        int mask;

    // ----------------------------------------

    /**
     * "单线程 OR 多线程" 生产者序号生成器
     */
    private final Sequencer sequencer;

    // ----------------------------------------

    /**
     * 避免伪共享, 右半部分填充
     */
    protected long p21, p22, p23, p24, p25, p26, p27;

    // =============================================================================

    /**
     * 创建 RingBuffer
     *
     * @param producerType 生产者类型(单线程生产者 OR 多线程生产者)
     * @param eventFactory 用户自定义的事件工厂
     * @param bufferSize   ringBuffer 容量
     * @param waitStrategy 指定的消费者阻塞策略
     */
    public static <E> RingBuffer<E> create(ProducerType producerType,
                                           EventFactory<E> eventFactory,
                                           int bufferSize,
                                           WaitStrategy waitStrategy)
    {
        switch (producerType)
        {
            case SINGLE:
            {
                return createSingleProducer(eventFactory, bufferSize, waitStrategy);
            }
            case MULTI:
            {
                return createMultiProducer(eventFactory, bufferSize, waitStrategy);
            }
            default:
                throw new RuntimeException("Un support producerType: " + producerType);
        }
    }

    public static <E> RingBuffer<E> createSingleProducer(EventFactory<E> factory,
                                                         int bufferSize,
                                                         WaitStrategy waitStrategy)
    {
        SingleProducerSequencer sequencer = new SingleProducerSequencer(bufferSize, waitStrategy);
        return new RingBuffer<>(factory, sequencer);
    }

    public static <E> RingBuffer<E> createMultiProducer(EventFactory<E> factory,
                                                        int bufferSize,
                                                        WaitStrategy waitStrategy)
    {
        MultiProducerSequencer sequencer = new MultiProducerSequencer(bufferSize, waitStrategy);
        return new RingBuffer<>(factory, sequencer);
    }

    @SuppressWarnings("unchecked")
    public RingBuffer(EventFactory<E> eventFactory, Sequencer sequencer)
    {
        int bufferSize = sequencer.getBufferSize();
        if (Integer.bitCount(bufferSize) != 1)
        {
            // ringBufferSize 需要是 2 的倍数, 类似 HashMap, 求余数时效率更高
            throw new IllegalArgumentException("BufferSize must be a power of 2");
        }

        // 保证数组的有效数据(引用)不会与其它数据位于同一缓存行, 避免伪共享
        this.entries    = (E[]) new Object[bufferSize + 2 * BUFFER_PAD];
        this.bufferSize = bufferSize;
        this.mask       = bufferSize - 1;
        this.sequencer  = sequencer;

        fill(eventFactory);
    }

    /**
     * <p>预填充事件对象
     * <p>数组中的对象可以一直复用, 在一定程度上减少垃圾回收, 但是该对象中封装的对象仍然会被垃圾回收
     */
    private void fill(EventFactory<E> eventFactory)
    {
        for (int i = 0; i < bufferSize; i++)
        {
            // 数组第一个有效数据的索引是 BUFFER_PAD
            // for 循环创建数组中的对象, 可以让这些对象在堆内存中的地址尽可能的连续一点
            entries[BUFFER_PAD + i] = eventFactory.newInstance();
        }
    }

    // =============================================================================

    public E get(long sequence)
    {
        // 由于 ringBuffer 的长度是 2 次幂, mask 为 2 次幂 - 1, 因此可以将求余运算优化为位运算
        int index = (int) (sequence & mask);
        return entries[BUFFER_PAD + index];
    }

    // =============================================================================

    public int getBufferSize()
    {
        return bufferSize;
    }

    /**
     * @see Sequencer#getCursor()
     */
    public Sequence getCursor()
    {
        return sequencer.getCursor();
    }

    // ----------------------------------------

    /**
     * @see Sequencer#newBarrier()
     */
    public SequenceBarrier newBarrier()
    {
        return sequencer.newBarrier();
    }

    /**
     * @see Sequencer#newBarrier(Sequence...)
     */
    public SequenceBarrier newBarrier(Sequence... dependenceSequences)
    {
        return sequencer.newBarrier(dependenceSequences);
    }

    /**
     * @see Sequencer#addGatingSequences(Sequence...)
     */
    public void addGatingSequences(Sequence... consumerSequences)
    {
        sequencer.addGatingSequences(consumerSequences);
    }

    /**
     * @see Sequencer#removeGatingSequence(Sequence)
     */
    public void removeGatingSequence(Sequence sequenceNeedRemove)
    {
        sequencer.removeGatingSequence(sequenceNeedRemove);
    }

    // ----------------------------------------

    /**
     * @see Sequencer#next()
     */
    public long next()
    {
        return sequencer.next();
    }

    /**
     * @see Sequencer#next(int)
     */
    public long next(int n)
    {
        return sequencer.next(n);
    }

    /**
     * @see Sequencer#publish(long)
     */
    public void publish(Long index)
    {
        sequencer.publish(index);
    }

    /**
     * @see Sequencer#publish(long, long) 
     */
    public void publish(long lo, long hi)
    {
        sequencer.publish(lo, hi);
    }

    // =============================================================================

    private void translateAndPublish(EventTranslatorVararg<E> translator, long sequence, Object... args)
    {
        try
        {
            translator.translateTo(get(sequence), sequence, args);
        }
        finally
        {
            sequencer.publish(sequence);
        }
    }

    private void translateAndPublishBatch(
            final EventTranslatorVararg<E> translator, int batchStartsAt,
            final int batchSize, final long finalSequence, final Object[][] args)
    {
        // args     in [batchStartsAt ....... batchEndsAt] batchSize
        // sequence in [initialSequence ... finalSequence] batchSize
        final long initialSequence = finalSequence - (batchSize - 1);
        try
        {
            long      sequence    = initialSequence;
            final int batchEndsAt = batchStartsAt + batchSize;
            for (int i = batchStartsAt; i < batchEndsAt; i++)
            {
                translator.translateTo(get(sequence), sequence++, args[i]);
            }
        }
        finally
        {
            sequencer.publish(initialSequence, finalSequence);
        }
    }

    // ----------------------------------------

    @Override
    public void publishEvent(EventTranslatorVararg<E> translator, Object... args)
    {
        final long sequence = sequencer.next();
        translateAndPublish(translator, sequence, args);
    }

    @Override
    public void publishEvents(EventTranslatorVararg<E> translator, Object[]... args)
    {
        publishEvents(translator, 0, args.length, args);
    }

    @Override
    public void publishEvents(EventTranslatorVararg<E> translator, int batchStartsAt, int batchSize, Object[]... args)
    {
        checkBounds(batchStartsAt, batchSize, args);
        final long finalSequence = sequencer.next(batchSize); // 目标待生产序号
        translateAndPublishBatch(translator, batchStartsAt, batchSize, finalSequence, args);
    }

    // =============================================================================

    /**
     * 检查 args[batchStartsAt ... batchStartsAt + batchSize - 1] 参数
     */
    private void checkBounds(final int batchStartsAt, final int batchSize, final Object[][] args)
    {
        checkBatchSizing(batchStartsAt, batchSize);
        batchOverRuns(args, batchStartsAt, batchSize);
    }

    private void checkBatchSizing(int batchStartsAt, int batchSize)
    {
        if (batchStartsAt < 0 || batchSize < 0)
        {
            throw new IllegalArgumentException("Both batchStartsAt and batchSize must be positive but got: batchStartsAt " + batchStartsAt + " and batchSize " + batchSize);
        }
        else if (batchSize > bufferSize)
        {
            throw new IllegalArgumentException("The ring buffer cannot accommodate " + batchSize + " it only has space for " + bufferSize + " entities.");
        }
    }

    private <A> void batchOverRuns(final A[] arg0, final int batchStartsAt, final int batchSize)
    {
        if (batchStartsAt + batchSize > arg0.length)
        {
            throw new IllegalArgumentException(
                    "A batchSize of: " + batchSize +
                            " with batchStatsAt of: " + batchStartsAt +
                            " will overrun the available number of arguments: " + (arg0.length - batchStartsAt));
        }
    }
}
