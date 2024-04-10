package com.zzw.relation.wait;

import com.zzw.relation.Sequence;

import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 阻塞等待策略
 */
public class BlockingWaitStrategy implements WaitStrategy {

    private final Lock lock = new ReentrantLock();
    private final Condition processorNotifyCondition = lock.newCondition();

    /**
     * <p>等待给定的序号可供使用, 由消费者调用
     * <p>类似 Condition.await, 如果不满足条件就会阻塞在该方法内不返回
     *
     * @param currentConsumeSequence  下一个需要消费的序号
     * @param currentProducerSequence 生产序号
     * @return 最大可消费序号
     */
    @Override
    public long waitFor(long currentConsumeSequence, Sequence currentProducerSequence) throws InterruptedException {
        long availableSequence; // 最大可消费序号

        // 如果 ringBuffer 的生产序号 < 当前所需消费序号, 说明目前消费速度 > 生产速度
        // 强一致的读生产序号, 看看生产者的生产进度是否推进了
        if ((availableSequence = currentProducerSequence.get()) < currentConsumeSequence) {
            lock.lock();
            try {
                while ((availableSequence = currentProducerSequence.get()) < currentConsumeSequence) {
                    // 消费速度 > 生产速度, 阻塞等待
                    processorNotifyCondition.await();
                }
            } finally {
                lock.unlock();
            }
        }

        // 跳出了上面的循环, 说明生产序号 >= 当前所需消费序号
        // currentProducerSequence >= currentConsumeSequence
        return availableSequence;
    }

    /**
     * 类似 Condition.signal, 唤醒 waitFor 阻塞在该等待策略对象上的消费者线程, 由生产者调用
     */
    @Override
    public void signalWhenBlocking() {
        lock.lock();
        try {
            // signal 唤醒所有阻塞在条件变量上的消费者线程(后续支持多消费者时, 会改为 signalAll)
            processorNotifyCondition.signal();
        } finally {
            lock.unlock();
        }
    }
}
