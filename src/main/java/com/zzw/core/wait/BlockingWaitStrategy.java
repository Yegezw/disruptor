package com.zzw.core.wait;

import com.zzw.core.Sequence;

import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class BlockingWaitStrategy implements WaitStrategy
{

    private final Lock      lock                     = new ReentrantLock();
    private final Condition processorNotifyCondition = lock.newCondition();

    @Override
    public long waitFor(long sequence, Sequence cursor) throws InterruptedException
    {
        // 最大可消费序号 = 已生产序号
        long availableSequence;

        // sequence <= availableSequence 才能申请
        if (sequence > (availableSequence = cursor.get()))
        {
            lock.lock();
            try
            {
                // 强一致获取已生产序号
                while (sequence > (availableSequence = cursor.get()))
                {
                    processorNotifyCondition.await();
                }
            } finally
            {
                lock.unlock();
            }
        }

        return availableSequence;
    }

    @Override
    public void signalAllWhenBlocking()
    {
        lock.lock();
        try
        {
            processorNotifyCondition.signal();
        } finally
        {
            lock.unlock();
        }
    }
}
