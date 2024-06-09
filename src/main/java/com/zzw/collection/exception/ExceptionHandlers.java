package com.zzw.collection.exception;

public class ExceptionHandlers
{

    public static ExceptionHandler<Object> defaultHandler()
    {
        return DefaultExceptionHandlerHolder.HANDLER;
    }

    private ExceptionHandlers()
    {
    }

    private static final class DefaultExceptionHandlerHolder
    {
        private static final ExceptionHandler<Object> HANDLER = new FatalExceptionHandler();
    }
}
