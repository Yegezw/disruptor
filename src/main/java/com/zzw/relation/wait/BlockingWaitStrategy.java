package com.zzw.relation.wait;

import com.zzw.relation.Sequence;
import com.zzw.util.SequenceUtil;

import java.util.List;
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
     * @param dependentSequenceList   当前消费者所依赖的上游消费者序号集合
     * @return 最大可消费序号
     */
    @Override
    public long waitFor(long currentConsumeSequence,
                        Sequence currentProducerSequence,
                        List<Sequence> dependentSequenceList) throws InterruptedException {
        // 如果 ringBuffer 的生产序号 < 当前所需消费序号, 说明目前消费速度 > 生产速度
        // 强一致的读生产序号, 看看生产者的生产进度是否推进了
        if (currentProducerSequence.get() < currentConsumeSequence) {
            lock.lock();
            try {
                while (currentProducerSequence.get() < currentConsumeSequence) {
                    // 消费速度 > 生产速度, 阻塞等待
                    processorNotifyCondition.await();
                }
            } finally {
                lock.unlock();
            }
        }
        // 跳出了上面的循环, 说明生产序号 >= 当前所需消费序号
        // currentProducerSequence >= currentConsumeSequence

        long availableSequence; // 最大可消费序号
        if (!dependentSequenceList.isEmpty()) {
            // 受制于屏障中的 dependentSequenceList
            // 用来控制当前消费者的消费进度 <= 其依赖的上游消费者的消费者进度
            while ((availableSequence = SequenceUtil.getMinimumSequence(dependentSequenceList)) < currentConsumeSequence) {
                // 由于消费者消费速度一般会很快, 所以这里使用自旋阻塞来等待上游消费者进度推进(响应及时且实现简单)
                // 在 JDK 9 开始引入的 Thread.onSpinWait 方法, 优化自旋性能
                ThreadHints.onSpinWait();
            }
        } else {
            // 并不存在依赖的上游消费者, 大于当前消费进度的生产者序号就是可用的消费序号
            availableSequence = currentProducerSequence.get();
        }

        return availableSequence;
    }

    /**
     * 类似 Condition.signal, 唤醒 waitFor 阻塞在该等待策略对象上的消费者线程, 由生产者调用
     */
    @Override
    public void signalWhenBlocking() {
        lock.lock();
        try {
            // signalAll 唤醒所有阻塞在条件变量上的消费者线程
            processorNotifyCondition.signalAll();
        } finally {
            lock.unlock();
        }
    }
}
