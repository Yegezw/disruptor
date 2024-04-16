package com.zzw.p4_util;

import sun.misc.Unsafe;

import java.lang.reflect.Field;

public class Util
{

    private Util()
    {
    }

    public static void sleep(long millis)
    {
        try
        {
            Thread.sleep(millis);
        } catch (InterruptedException e)
        {
            throw new RuntimeException(e);
        }
    }

    private static final Unsafe UNSAFE;

    static
    {
        try
        {
            Field theUnsafe = Unsafe.class.getDeclaredField("theUnsafe");
            theUnsafe.setAccessible(true);
            UNSAFE = (Unsafe) theUnsafe.get(null);
        } catch (Exception e)
        {
            throw new RuntimeException("Loading Unsafe failed");
        }
    }

    public static Unsafe getUnsafe()
    {
        return UNSAFE;
    }
}
