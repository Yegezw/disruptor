package com.zzw.relation.wait;

/**
 * 被唤醒异常, 用于消费者优雅停止
 */
public final class AlertException extends Exception
{

    /**
     * 单例
     */
    public static final AlertException INSTANCE = new AlertException();

    private AlertException()
    {
    }

    /**
     * <p>不会为此异常填充堆栈追踪信息 stackTrace
     * <p>
     * 1、使用 new 创建异常: 生成栈追踪信息 stackTrace<br>
     * 2、使用 throw 抛出异常: 栈展开<br>
     * 3、打印异常调用链 stackTrace
     */
    @Override
    public Throwable fillInStackTrace()
    {
        return this;
    }
}
