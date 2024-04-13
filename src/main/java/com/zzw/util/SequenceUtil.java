package com.zzw.util;

import com.zzw.relation.Sequence;

/**
 * 序号工具类
 */
public class SequenceUtil {

    /**
     * 从 dependentSequences 和 minimumSequence 中获得最小的序号
     *
     * @param minimumSequence    申请的目标生产序号
     * @param dependentSequences 依赖的序号数组
     */
    public static long getMinimumSequence(Sequence[] dependentSequences, long minimumSequence) {
        for (Sequence sequence : dependentSequences) {
            long value = sequence.get();
            minimumSequence = Math.min(minimumSequence, value);
        }

        return minimumSequence;
    }

    /**
     * 获得传入的序号数组中的最小序号
     *
     * @param dependentSequences 依赖的序号数组
     */
    public static long getMinimumSequence(Sequence[] dependentSequences) {
        // Long.MAX_VALUE 作为上界, 即使 dependentSequences 为空, 也会返回一个 Long.MAX_VALUE 作为最小序列号
        return getMinimumSequence(dependentSequences, Long.MAX_VALUE);
    }
}
