package reactivestreams.commons.util;

import java.util.concurrent.atomic.AtomicLongFieldUpdater;

public enum BackpressureHelper {
    ;

    public static long addCap(long a, long b) {
        long u = a + b;
        if (u < 0) {
            return Long.MAX_VALUE;
        }
        return u;
    }

    public static long multiplyCap(long a, long b) {
        long u = a * b;
        if (((a | b) >>> 31) != 0 && (b != 0 && u / b != a)) {
            return Long.MAX_VALUE;
        }
        return u;
    }

    /**
     * Atomically adds the value to the atomic variable, capping the sum at Long.MAX_VALUE
     * and returning the original value.
     * @param <T> the type of the parent class of the field
     * @param updater the field updater
     * @param instance the instance of the field to update
     * @param n the value to add, n > 0, not validated
     * @return the original value before the add
     */
    public static <T> long getAndAddCap(AtomicLongFieldUpdater<T> updater, T instance, long n) {
        for (; ; ) {
            long r = updater.get(instance);
            if (r == Long.MAX_VALUE) {
                return Long.MAX_VALUE;
            }
            long u = addCap(r, n);
            if (updater.compareAndSet(instance, r, u)) {
                return r;
            }
        }
    }
}
