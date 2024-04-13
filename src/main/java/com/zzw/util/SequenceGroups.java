package com.zzw.util;

import com.zzw.relation.Sequence;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

/**
 * <p>更新 Sequence 数组的工具类
 * <p>实现中 CAS 的插入 / 删除机制, 在 Disruptor 中是不必要的, 因为 Disruptor 不支持在运行时动态的注册新消费者
 * <p>官方 Disruptor 支持, 但是有一些额外的复杂度, 这里只是为了和官方 Disruptor 的实现保持一致, 说明实现原理
 * <p>我们的 Disruptor 本质上只需要支持 Sequence[] 的扩容 / 缩容即可
 */
public class SequenceGroups {

    /**
     * 为 holder.Sequence[] 添加 sequencesToAdd
     */
    public static <T> void addSequences(
            final T holder,
            final AtomicReferenceFieldUpdater<T, Sequence[]> updater,
            final Sequence currentProducerSequence,
            final Sequence... sequencesToAdd) {
        long cursorSequence;
        Sequence[] updatedSequences;
        Sequence[] currentSequences;

        do {
            // 获得 holder.Sequence[]
            currentSequences = updater.get(holder);
            // 将原数组中的数据复制到新的数组中
            updatedSequences = Arrays.copyOf(currentSequences, currentSequences.length + sequencesToAdd.length);

            // 生产者的生产序号
            cursorSequence = currentProducerSequence.get();
            // 每个新添加的 sequence 值都以 cursorSequence 为准
            int index = currentSequences.length;
            for (Sequence sequence : sequencesToAdd) {
                sequence.set(cursorSequence);
                updatedSequences[index++] = sequence;
            }

            // CAS 的将新数组赋值给对象, 允许 disruptor 在运行时并发的注册新的消费者 Sequence[]
            // 只有 CAS 赋值成功才会返回, 失败的话会重新获取最新的 currentSequences, 重新构建、合并新的 updatedSequences 数组
        } while (!updater.compareAndSet(holder, currentSequences, updatedSequences));

        // 新注册的消费序号, 再以生产者的生产序号为准, 做一次最终修正
        cursorSequence = currentProducerSequence.get();
        for (Sequence sequence : sequencesToAdd) {
            sequence.set(cursorSequence);
        }
    }

    /**
     * 为 holder.Sequence[] 删除 sequenceNeedRemove
     */
    public static <T> void removeSequence(
            final T holder,
            final AtomicReferenceFieldUpdater<T, Sequence[]> sequenceUpdater,
            final Sequence sequenceNeedRemove) {
        int numToRemove;
        Sequence[] oldSequences;
        Sequence[] newSequences;

        do {
            // 获得 holder.Sequence[]
            oldSequences = sequenceUpdater.get(holder);
            // 获得需要从数组中删除的 sequenceNeedRemove 个数
            numToRemove = countMatching(oldSequences, sequenceNeedRemove);

            // 没找到需要删除的 sequenceNeedRemove, 直接返回
            if (0 == numToRemove) {
                return;
            }

            // 构造新的 gatingConsumerSequences
            final int oldSize = oldSequences.length;
            newSequences = new Sequence[oldSize - numToRemove];

            // 将原数组中的 "非 sequenceNeedRemove" 复制到新数组中
            for (int i = 0, pos = 0; i < oldSize; i++) {
                final Sequence testSequence = oldSequences[i];
                if (sequenceNeedRemove != testSequence) {
                    newSequences[pos++] = testSequence;
                }
            }
        } while (!sequenceUpdater.compareAndSet(holder, oldSequences, newSequences));
    }

    private static int countMatching(Sequence[] values, final Sequence toMatch) {
        int numToRemove = 0;

        // 比对 Sequence 引用, 如果和 toMatch 相同, 则需要删除
        for (Sequence value : values) {
            if (value == toMatch) {
                numToRemove++;
            }
        }

        return numToRemove;
    }
}
