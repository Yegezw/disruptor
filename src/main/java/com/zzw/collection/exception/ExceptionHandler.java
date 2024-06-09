package com.zzw.collection.exception;

public interface ExceptionHandler<T>
{

    void handleEventException(Throwable ex, long sequence, T event);

    void handleOnShutdownException(Throwable ex);
}
