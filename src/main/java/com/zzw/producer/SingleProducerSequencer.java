package com.zzw.producer;

import com.zzw.relation.Sequence;
import com.zzw.relation.SequenceBarrier;
import com.zzw.relation.wait.WaitStrategy;
import com.zzw.util.SequenceGroups;
import com.zzw.util.SequenceUtil;

import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.concurrent.locks.LockSupport;

/**
 * 单线程生产者序号生成器
 */
public class SingleProducerSequencer implements Sequencer
{

    /**
     * 生产者序号生成器所属 RingBuffer 的大小
     */
    private final int      bufferSize;
    /**
     * 当前已发布的生产序号(区别于 nextValue)
     */
    private final Sequence cursor = new Sequence();

    // ----------------------------------------

    /**
     * gatingSequences 原子更新器
     */
    private static final AtomicReferenceFieldUpdater<SingleProducerSequencer, Sequence[]> SEQUENCE_UPDATER =
            AtomicReferenceFieldUpdater.newUpdater(
                    SingleProducerSequencer.class,
                    Sequence[].class,
                    "gatingSequences"
            );

    /**
     * 生产者序号生成器所属 RingBuffer 的消费者序号数组
     */
    @SuppressWarnings("all")
    private volatile Sequence[]   gatingSequences = new Sequence[0];
    /**
     * 消费者等待策略
     */
    private final    WaitStrategy waitStrategy;

    // ----------------------------------------

    /**
     * 避免伪共享, 左半部分填充
     */
    protected long p11, p12, p13, p14, p15, p16, p17;

    /**
     * <p>已申请的序号(是否发布了, 要看 cursor)
     * <p>单线程生产者内部使用, 所以就是普通的 long, 不考虑并发
     */
    private long nextValue   = -1;
    /**
     * <p>当前已缓存的消费序号
     * <p>单线程生产者内部使用, 所以就是普通的 long, 不考虑并发
     */
    private long cachedValue = -1;

    /**
     * 避免伪共享, 右半部分填充
     */
    protected long p21, p22, p23, p24, p25, p26, p27;

    // =============================================================================

    public SingleProducerSequencer(int bufferSize, WaitStrategy WaitStrategy)
    {
        this.bufferSize   = bufferSize;
        this.waitStrategy = WaitStrategy;
    }

    @Override
    public int getBufferSize()
    {
        return bufferSize;
    }

    @Override
    public Sequence getCursor()
    {
        return cursor;
    }

    // =============================================================================

    @Override
    public SequenceBarrier newBarrier()
    {
        return new SequenceBarrier(this, waitStrategy, cursor, new Sequence[0]);
    }

    @Override
    public SequenceBarrier newBarrier(Sequence... sequencesToTrack)
    {
        return new SequenceBarrier(this, waitStrategy, cursor, sequencesToTrack);
    }

    @Override
    public void addGatingSequences(Sequence... gatingSequences)
    {
        SequenceGroups.addSequences(this, SEQUENCE_UPDATER, cursor, gatingSequences);
    }

    @Override
    public void removeGatingSequence(Sequence sequence)
    {
        SequenceGroups.removeSequence(this, SEQUENCE_UPDATER, sequence);
    }

    // =============================================================================

    @Override
    public long next()
    {
        return next(1);
    }

    @Override
    public long next(int n)
    {
        long nextValue = this.nextValue; // 已申请序号

        // 目标生产序号
        long nextSequence = nextValue + n;
        // 上一轮覆盖点 <= 消费序号
        long wrapPoint = nextSequence - bufferSize;

        // 获得当前已缓存的消费序号
        long cachedGatingSequence = this.cachedValue;

        // wrapPoint <= cachedGatingSequence 才是可以申请的
        // 消费序号 consumerSequence 并不是实时获取的(因为在没有超过环绕点一圈时, 生产者是可以放心生产的)
        // 每次申请生产序号都实时获取消费序号, 会触发对消费者 sequence 强一致的读, 迫使消费者线程所在的 CPU 刷新缓存(而这是不需要的)
        if (wrapPoint > cachedGatingSequence)
        {
            // 比起 disruptor 省略了 if 中的 cachedGatingSequence > nextProducerSequence 逻辑
            // 原因请见: https://github.com/LMAX-Exchange/disruptor/issues/76

            // 比起 disruptor 省略了 currentProducerSequence.set(nextProducerSequence);
            // 原因请见: https://github.com/LMAX-Exchange/disruptor/issues/291
            long minSequence;

            // 当生产者发现已超过消费者一圈, 就必须去读最新的消费者序号了, 看看消费者的消费进度是否推进了
            // 这里的 getMinimumSequence 是对 volatile 变量的读, 是实时的、强一致的读
            while (wrapPoint > (minSequence = SequenceUtil.getMinimumSequence(gatingSequences, nextValue)))
            {
                // 消费进度没有推进, 则生产者无法获取可用的队列空间, 循环的间歇性 park 阻塞
                // 如果消费者消费速度比较慢, 那么生产者线程将长时间的处于自旋状态, 严重浪费 CPU 资源
                // 因此使用 next(n) 方式获取生产者序号时, 用户必须保证消费者有足够的消费速度
                LockSupport.parkNanos(1L);
            }

            // 满足条件了, 则缓存获得最新的消费者序号
            // 因为不是实时获取消费序号, minSequence 可能比 cachedValue 大很多
            // 这种情况下, 待到下一次 next 申请时就可以不用去强一致的读 consumerSequence 了
            this.cachedValue = minSequence;
        }

        // 记录本次申请后的目标生产序号
        this.nextValue = nextSequence;

        return nextSequence;
    }

    @Override
    public void publish(long sequence)
    {
        // 发布时, 更新生产者队列
        // lazySet 保证 publish() 执行前, 生产者对事件对象更新的写操作, 一定先于对生产者 Sequence 的更新
        // lazySet 由于消费者可以批量的拉取数据, 所以不必每次发布时都 volatile 的更新, 允许消费者晚一点感知到, 这样性能会更好
        cursor.set(sequence);

        // 发布完成后, 唤醒可能阻塞等待的消费者线程
        waitStrategy.signalAllWhenBlocking();
    }

    @Override
    public void publish(long lo, long hi)
    {
        publish(hi);
    }

    // =============================================================================

    @Override
    public long getHighestPublishedSequence(long lowerBound, long availableSequence)
    {
        return availableSequence;
    }
}
