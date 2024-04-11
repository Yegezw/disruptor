package com.zzw.producer;

import com.zzw.relation.Sequence;
import com.zzw.relation.SequenceBarrier;
import com.zzw.relation.wait.WaitStrategy;
import com.zzw.util.SequenceUtil;
import com.zzw.util.Util;
import sun.misc.Unsafe;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.locks.LockSupport;

/**
 * 多线程生产者序号生成器
 */
public class MultiProducerSequencer implements Sequencer {

    /**
     * 生产序号生成器所属 RingBuffer 的大小
     */
    private final int bufferSize;
    /**
     * 多线程生产者共同的已申请序号(可能未发布)
     */
    private final Sequence currentProducerSequence = new Sequence();

    // ----------------------------------------

    /**
     * 生产序号生成器所属 RingBuffer 的消费序号集合
     */
    private final List<Sequence> gatingConsumerSequenceList = new ArrayList<>();
    /**
     * 消费者等待策略
     */
    private final WaitStrategy waitStrategy;
    /**
     * <p>缓存的最小消费序号, 并不是实时获取的
     * <p>每次申请生产序号都实时获取消费序号<br>
     * 会触发对消费者 sequence 强一致的读, 迫使消费者线程所在的 CPU 刷新缓存, 而这是不需要的
     */
    private final Sequence gatingSequenceCache = new Sequence();

    // ----------------------------------------

    /**
     * 初始值为 -1, 标识 ringBuffer 中对应下标位置的事件第几次被覆盖
     */
    private final int[] availableBuffer;
    private final int indexMask;
    private final int indexShift; // 2 ^ indexShift = bufferSize

    /**
     * 通过 unsafe 访问 availableBuffer 数组, 可以在读写时按需插入 "读 OR 写" 内存屏障
     */
    private static final Unsafe UNSAFE = Util.getUnsafe();
    private static final long BASE = UNSAFE.arrayBaseOffset(int[].class);
    private static final long SCALE = UNSAFE.arrayIndexScale(int[].class);

    // =============================================================================

    public MultiProducerSequencer(int bufferSize, final WaitStrategy waitStrategy) {
        this.bufferSize = bufferSize;
        this.waitStrategy = waitStrategy;

        this.availableBuffer = new int[bufferSize];
        this.indexMask = bufferSize - 1;
        this.indexShift = log2(bufferSize);
        initialiseAvailableBuffer();
    }

    private void initialiseAvailableBuffer() {
        for (int i = availableBuffer.length - 1; i != 0; i--) {
            setAvailableBufferValue(i, -1);
        }

        setAvailableBufferValue(0, -1);
    }

    private static int log2(int i) {
        int r = 0;
        while ((i >>= 1) != 0) {
            ++r;
        }
        return r;
    }

    // =============================================================================

    @Override
    public int getBufferSize() {
        return bufferSize;
    }

    @Override
    public Sequence getCurrentProducerSequence() {
        return currentProducerSequence;
    }

    // ----------------------------------------

    @Override
    public SequenceBarrier newBarrier() {
        return new SequenceBarrier(this, currentProducerSequence, waitStrategy, new ArrayList<>());
    }

    @Override
    public SequenceBarrier newBarrier(Sequence... dependenceSequences) {
        return new SequenceBarrier(this, currentProducerSequence, waitStrategy, new ArrayList<>(Arrays.asList(dependenceSequences)));

    }

    @Override
    public void addGatingConsumerSequence(Sequence newGatingConsumerSequence) {
        gatingConsumerSequenceList.add(newGatingConsumerSequence);
    }

    @Override
    public void addGatingConsumerSequenceList(Sequence... newGatingConsumerSequences) {
        gatingConsumerSequenceList.addAll(Arrays.asList(newGatingConsumerSequences));
    }

    @Override
    public void removeGatingConsumerSequence(Sequence sequenceNeedRemove) {
        gatingConsumerSequenceList.remove(sequenceNeedRemove);
    }

    // ----------------------------------------

    @Override
    public long next() {
        return next(1);
    }

    @Override
    public long next(int n) {
        long current; // 多线程共同的已申请序号(可能未发布)
        long next;    // 目标生产序号

        do {
            current = currentProducerSequence.get();
            next = current + n;

            // volatile 读 gatingSequenceCache, 因为多生产者环境下会并发读写 gatingSequenceCache
            long wrapPoint = next - bufferSize;                    // 上一轮覆盖点
            long cachedGatingSequence = gatingSequenceCache.get(); // 缓存的最小消费序号

            // wrapPoint <= cachedGatingSequence 才是可以申请的
            // 当生产者发现已超过消费者一圈, 就必须去读最新的消费序号了, 看看消费者的消费进度是否推进了
            if (wrapPoint > cachedGatingSequence) {
                long gatingSequence = SequenceUtil.getMinimumSequence(gatingConsumerSequenceList, current);

                if (wrapPoint > gatingSequence) {
                    LockSupport.parkNanos(1);
                    continue;
                }

                gatingSequenceCache.set(gatingSequence);
            } else if (currentProducerSequence.compareAndSet(current, next)) break;
        } while (true);

        return next;
    }

    @Override
    public void publish(long publishIndex) {
        setAvailable(publishIndex);
        waitStrategy.signalWhenBlocking();
    }

    // ----------------------------------------

    /**
     * 获取 "连续的 + 已发布的 + 最大的" 生产序号
     *
     * @param lowBound          下一个需要消费的序号
     * @param availableSequence 最大可消费序号
     * @return "连续的 + 已发布的 + 最大的" 生产序号
     */
    @Override
    public long getHighestPublishedSequence(long lowBound, long availableSequence) {
        // lowBound 是消费者传入的, 保证是已发布的生产序号

        // lowBound 和 availableSequence 中间存在未发布的生产序号
        for (long sequence = lowBound; sequence <= availableSequence; sequence++) {
            if (!isAvailable(sequence)) {
                return sequence - 1;
            }
        }

        // lowBound 和 availableSequence 中间不存在未发布的生产序号
        return availableSequence;
    }

    // =============================================================================

    public boolean isAvailable(long sequence) {
        int index = calculateIndex(sequence);           // sequence 下标
        int flag = calculateAvailabilityFlag(sequence); // sequence 覆盖次数
        long bufferAddress = (index * SCALE) + BASE;    // availableBuffer[index] 地址

        // 功能上等价于 availableBuffer[index] == flag, 但添加了读屏障
        // 保证了强一致的读, 可以让消费者实时的获取到生产者最新发布的事件
        return UNSAFE.getIntVolatile(availableBuffer, bufferAddress) == flag; // Volatile 读 availableBuffer[index]
    }

    private void setAvailable(long sequence) {
        int index = calculateIndex(sequence);           // sequence 下标
        int flag = calculateAvailabilityFlag(sequence); // sequence 覆盖次数
        setAvailableBufferValue(index, flag);
    }

    private void setAvailableBufferValue(int index, int flag) {
        // 功能上等价于 availableBuffer[index] = flag, 但添加了写屏障
        // 保证 publish() 执行前, 生产者对事件对象更新的写操作, 一定先于对 availableBuffer[index] 的更新
        long bufferAddress = (index * SCALE) + BASE;                // availableBuffer[index] 地址
        UNSAFE.putOrderedInt(availableBuffer, bufferAddress, flag); // StoreStore 写 availableBuffer[index]
    }

    /**
     * sequence 覆盖次数
     */
    private int calculateAvailabilityFlag(long sequence) {
        // sequence / bufferSize
        return (int) (sequence >>> indexShift);
    }

    /**
     * 返回 sequence 下标
     */
    private int calculateIndex(long sequence) {
        // sequence % bufferSize
        return ((int) sequence) & indexMask;
    }
}
