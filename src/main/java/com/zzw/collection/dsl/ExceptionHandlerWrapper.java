package com.zzw.collection.dsl;

import com.zzw.collection.exception.ExceptionHandler;
import com.zzw.collection.exception.ExceptionHandlers;

public class ExceptionHandlerWrapper<T> implements ExceptionHandler<T>
{

    /**
     * 委托
     */
    private ExceptionHandler<? super T> delegate;

    public void switchTo(final ExceptionHandler<? super T> exceptionHandler)
    {
        this.delegate = exceptionHandler;
    }

    private ExceptionHandler<? super T> getExceptionHandler()
    {
        ExceptionHandler<? super T> handler = delegate;
        if (handler == null)
        {
            return ExceptionHandlers.defaultHandler();
        }
        return handler;
    }

    @Override
    public void handleEventException(Throwable ex, long sequence, T event)
    {
        getExceptionHandler().handleEventException(ex, sequence, event);
    }

    @Override
    public void handleOnShutdownException(Throwable ex)
    {
        getExceptionHandler().handleOnShutdownException(ex);
    }
}
