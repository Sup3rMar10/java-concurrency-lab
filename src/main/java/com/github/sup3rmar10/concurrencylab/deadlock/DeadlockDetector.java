package com.github.sup3rmar10.concurrencylab.deadlock;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.util.Arrays;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

/** JVM-wide deadlock check (monitors and java.util.concurrent locks) via ThreadMXBean. */
public final class DeadlockDetector {

    public static Set<String> deadlockedThreadNames() {
        ThreadMXBean bean = ManagementFactory.getThreadMXBean();
        long[] ids = bean.findDeadlockedThreads(); // null when there is none
        if (ids == null) return Set.of();
        return Arrays.stream(bean.getThreadInfo(ids))
                .filter(Objects::nonNull)
                .map(ThreadInfo::getThreadName)
                .collect(Collectors.toCollection(TreeSet::new));
    }

    private DeadlockDetector() { }
}
