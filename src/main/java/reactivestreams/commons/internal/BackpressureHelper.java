package reactivestreams.commons.internal;

import java.util.concurrent.atomic.*;

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
    
    public static long add(AtomicLong requested, long n) {
        for (;;) {
            long r = requested.get();
            if (r == Long.MAX_VALUE) {
                return Long.MAX_VALUE;
            }
            long u = addCap(r, n);
            if (requested.compareAndSet(r, u)) {
                return r;
            }
        }
    }
    
    public static <T> long add(AtomicLongFieldUpdater<T> updater, T instance, long n) {
        for (;;) {
            long r = updater.get(instance);
            if (r == Long.MAX_VALUE) {
                return Long.MAX_VALUE;
            }
            long u = addCap(r, n);
            if (updater.compareAndSet(instance, r, u)) {
                return u;
            }
        }
    }
    
    public static void reportBadRequest(long n) {
        new IllegalArgumentException("Request amount must be positive but it is " + n).printStackTrace();
    }
    
    public static boolean validate(long n) {
        if (n <= 0) {
            reportBadRequest(n);
            return false;
        }
        return true;
    }
}
