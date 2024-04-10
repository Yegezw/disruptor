package com.zzw.relation.wait;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;

import static java.lang.invoke.MethodType.methodType;

public class ThreadHints {

    private static final MethodHandle ON_SPIN_WAIT_METHOD_HANDLE;

    static {
        final MethodHandles.Lookup lookup = MethodHandles.lookup();

        MethodHandle methodHandle = null;
        try {
            methodHandle = lookup.findStatic(Thread.class, "onSpinWait", methodType(void.class));
        } catch (final Exception ignore) {
            // JDK 9 才引入的 Thread.onSpinWait, 低版本没找到该方法直接忽略异常即可
        }

        ON_SPIN_WAIT_METHOD_HANDLE = methodHandle;
    }

    public static void onSpinWait() {
        if (null != ON_SPIN_WAIT_METHOD_HANDLE) {
            try {
                // 如果是高版本 JDK 找到了 Thread.onSpinWait 方法
                // 则进行调用, 插入特殊指令优化 CPU 自旋性能, 例如 x86 架构中的 pause 汇编指令
                // invokeExact 比起反射调用方法要高一些, 详细的原因待研究
                ON_SPIN_WAIT_METHOD_HANDLE.invokeExact();
            } catch (final Throwable ignore) {
                // 异常无需考虑
            }
        }
    }
}
