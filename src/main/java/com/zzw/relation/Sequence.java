package com.zzw.relation;

import com.zzw.util.Util;
import sun.misc.Unsafe;

/**
 * <p>序号对象
 * <p>由于需要被 "生产者线程 + 消费者线程" 同时访问, 因此内部是一个 volatile 修饰的 long 值
 */
public class Sequence {

    /**
     * 避免伪共享, 左半部分填充
     */
    protected long p11, p12, p13, p14, p15, p16, p17;

    /**
     * 序号起始值默认为 -1, 保证下一序号恰好是 0(即第一个合法的序号)
     */
    private volatile long value = -1;

    /**
     * 避免伪共享, 右半部分填充
     */
    protected long p21, p22, p23, p24, p25, p26, p27;

    // ----------------------------------------

    private static final Unsafe UNSAFE;
    private static final long VALUE_OFFSET;

    static {
        UNSAFE = Util.getUnsafe();
        try {
            VALUE_OFFSET = UNSAFE.objectFieldOffset(Sequence.class.getDeclaredField("value"));
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
    }

    // =============================================================================

    public Sequence() {
    }

    public Sequence(long value) {
        this.value = value;
    }

    // =============================================================================

    public long get() {
        return value;
    }

    public void set(long value) {
        this.value = value;
    }

    /**
     * 对 value 的写操作, 只保证有序性, 不保证可见性
     */
    public void lazySet(long value) {
        // StoreStore barrier
        // this.value = value
        // 不会立即强制 CPU 刷新缓存, 导致其修改的最新值对其它 CPU 核心来说不是立即可见的(延迟几纳秒)
        UNSAFE.putOrderedLong(this, VALUE_OFFSET, value);
    }

    public boolean compareAndSet(long expect, long update) {
        return UNSAFE.compareAndSwapLong(this, VALUE_OFFSET, expect, update);
    }
}
