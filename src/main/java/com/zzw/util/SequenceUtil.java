package com.zzw.util;

import com.zzw.relation.Sequence;

import java.util.List;

/**
 * 序号工具类
 */
public class SequenceUtil {

    /**
     * 从 dependentSequenceList 和 minimumSequence 中获得最小的序号
     *
     * @param minimumSequence       申请的目标生产序号
     * @param dependentSequenceList 依赖的序号集合
     */
    public static long getMinimumSequence(List<Sequence> dependentSequenceList, long minimumSequence) {
        for (Sequence sequence : dependentSequenceList) {
            long value = sequence.get();
            minimumSequence = Math.min(minimumSequence, value);
        }

        return minimumSequence;
    }

    /**
     * 获得传入的序号集合中的最小序号
     *
     * @param dependentSequenceList 依赖的序号集合
     */
    public static long getMinimumSequence(List<Sequence> dependentSequenceList) {
        // Long.MAX_VALUE 作为上界, 即使 dependentSequenceList 为空, 也会返回一个 Long.MAX_VALUE 作为最小序列号
        return getMinimumSequence(dependentSequenceList, Long.MAX_VALUE);
    }
}
