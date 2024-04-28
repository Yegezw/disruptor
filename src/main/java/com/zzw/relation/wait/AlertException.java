package com.zzw.relation.wait;

/**
 * 被唤醒异常, 用于消费者优雅停止
 */
public class AlertException extends Exception
{

    /**
     * 单例
     */
    public static final AlertException INSTANCE = new AlertException();
}
