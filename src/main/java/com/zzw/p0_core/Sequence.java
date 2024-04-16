package com.zzw.p0_core;

import com.zzw.p4_util.Util;
import sun.misc.Unsafe;

/**
 * 序号
 */
public class Sequence
{

    private static final Unsafe UNSAFE;
    private static final long   VALUE_OFFSET;
    public static final  long   INITIAL_VALUE = -1;

    static
    {
        UNSAFE = Util.getUnsafe();
        try
        {
            VALUE_OFFSET = UNSAFE.objectFieldOffset(Sequence.class.getDeclaredField("value"));
        } catch (Exception e)
        {
            throw new RuntimeException(e);
        }
    }

    // =============================================================================

    private volatile long value;

    public Sequence()
    {
        this.value = INITIAL_VALUE;
    }

    public Sequence(long initialValue)
    {
        UNSAFE.putOrderedLong(this, VALUE_OFFSET, initialValue);
    }

    // =============================================================================

    /**
     * 保证可见有序性, 不保证可见性
     */
    public void set(long value)
    {
        // StoreStore barrier
        // this.value = value
        UNSAFE.putOrderedLong(this, VALUE_OFFSET, value);
    }

    /**
     * 保证可见有序性 + 可见性
     */
    public void setVolatile(long value)
    {
        // StoreStore barrier
        // this.value = value
        // StoreLoad  barrier
        UNSAFE.putLongVolatile(this, VALUE_OFFSET, value);
    }

    /**
     * 强一致读
     */
    public long get()
    {
        // read value
        // LoadLoad
        // LoadStore
        return value;
    }
}
