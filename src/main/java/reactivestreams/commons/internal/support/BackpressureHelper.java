package reactivestreams.commons.internal.support;

import org.reactivestreams.Subscriber;

import java.util.Queue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.function.BooleanSupplier;

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

    public static long addAndGet(AtomicLong requested, long n) {
        for (; ; ) {
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

    public static <T> long addAndGet(AtomicLongFieldUpdater<T> updater, T instance, long n) {
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

    public static void reportBadRequest(long n) {
        new IllegalArgumentException("Request amount must be positive but it is " + n).printStackTrace();
    }

    static final long COMPLETED_MASK = 0x8000_0000_0000_0000L;
    static final long REQUESTED_MASK = 0x7FFF_FFFF_FFFF_FFFFL;

    /**
     * Perform a potential post-completion request accounting.
     *
     * @param n
     * @param actual
     * @param queue
     * @param state
     * @param isCancelled
     * @return true if the state indicates a completion state.
     */
    public static <T> boolean postCompleteRequest(long n,
                                                  Subscriber<? super T> actual,
                                                  Queue<T> queue,
                                                  AtomicLong state,
                                                  BooleanSupplier isCancelled) {
        for (; ; ) {
            long r = state.get();

            // extract the current request amount
            long r0 = r & REQUESTED_MASK;

            // preserve COMPLETED_MASK and calculate new requested amount
            long u = (r & COMPLETED_MASK) | BackpressureHelper.addCap(r0, n);

            if (state.compareAndSet(r, u)) {
                // (complete, 0) -> (complete, n) transition then replay
                if (r == COMPLETED_MASK) {

                    postCompleteDrain(n | COMPLETED_MASK, actual, queue, state, isCancelled);

                    return true;
                }
                // (active, r) -> (active, r + n) transition then continue with requesting from upstream
                return false;
            }
        }

    }

    /**
     * Drains the queue either in a pre- or post-complete state.
     *
     * @param n
     * @param actual
     * @param queue
     * @param state
     * @param isCancelled
     * @return true if the queue was completely drained or the drain process was cancelled
     */
    static <T> boolean postCompleteDrain(long n,
                                         Subscriber<? super T> actual,
                                         Queue<T> queue,
                                         AtomicLong state,
                                         BooleanSupplier isCancelled) {

// TODO enable fast-path
//        if (n == -1 || n == Long.MAX_VALUE) {
//            for (;;) {
//                if (isCancelled.getAsBoolean()) {
//                    break;
//                }
//                
//                T v = queue.poll();
//                
//                if (v == null) {
//                    actual.onComplete();
//                    break;
//                }
//                
//                actual.onNext(v);
//            }
//            
//            return true;
//        }

        long e = n & COMPLETED_MASK;

        for (; ; ) {

            while (e != n) {
                if (isCancelled.getAsBoolean()) {
                    return true;
                }

                T t = queue.poll();

                if (t == null) {
                    actual.onComplete();
                    return true;
                }

                actual.onNext(t);
                e++;
            }

            if (isCancelled.getAsBoolean()) {
                return true;
            }

            if (queue.isEmpty()) {
                actual.onComplete();
                return true;
            }

            n = state.get();

            if (n == e) {

                n = state.addAndGet(-(e & REQUESTED_MASK));

                if ((n & REQUESTED_MASK) == 0L) {
                    return false;
                }

                e = n & COMPLETED_MASK;
            }
        }

    }

    /**
     * Tries draining the queue if the source just completed.
     *
     * @param actual
     * @param queue
     * @param state
     * @param isCancelled
     */
    public static <T> void postComplete(Subscriber<? super T> actual,
                                        Queue<T> queue,
                                        AtomicLong state,
                                        BooleanSupplier isCancelled) {

        if (queue.isEmpty()) {
            actual.onComplete();
            return;
        }

        if (postCompleteDrain(state.get(), actual, queue, state, isCancelled)) {
            return;
        }

        for (; ; ) {
            long r = state.get();

            if ((r & COMPLETED_MASK) != 0L) {
                return;
            }

            long u = r | COMPLETED_MASK;
            // (active, r) -> (complete, r) transition
            if (state.compareAndSet(r, u)) {
                // if the requested amount was non-zero, drain the queue
                if (r != 0L) {
                    postCompleteDrain(u, actual, queue, state, isCancelled);
                }

                return;
            }
        }

    }

    /**
     * Perform a potential post-completion request accounting.
     *
     * @param n
     * @param actual
     * @param queue
     * @param state
     * @param isCancelled
     * @return true if the state indicates a completion state.
     */
    public static <T, F> boolean postCompleteRequest(long n,
                                                     Subscriber<? super T> actual,
                                                     Queue<T> queue,
                                                     AtomicLongFieldUpdater<F> field,
                                                     F instance,
                                                     BooleanSupplier isCancelled) {
        for (; ; ) {
            long r = field.get(instance);

            // extract the current request amount
            long r0 = r & REQUESTED_MASK;

            // preserve COMPLETED_MASK and calculate new requested amount
            long u = (r & COMPLETED_MASK) | BackpressureHelper.addCap(r0, n);

            if (field.compareAndSet(instance, r, u)) {
                // (complete, 0) -> (complete, n) transition then replay
                if (r == COMPLETED_MASK) {

                    postCompleteDrain(n | COMPLETED_MASK, actual, queue, field, instance, isCancelled);

                    return true;
                }
                // (active, r) -> (active, r + n) transition then continue with requesting from upstream
                return false;
            }
        }

    }

    /**
     * Drains the queue either in a pre- or post-complete state.
     *
     * @param n
     * @param actual
     * @param queue
     * @param state
     * @param isCancelled
     * @return true if the queue was completely drained or the drain process was cancelled
     */
    static <T, F> boolean postCompleteDrain(long n,
                                            Subscriber<? super T> actual,
                                            Queue<T> queue,
                                            AtomicLongFieldUpdater<F> field,
                                            F instance,
                                            BooleanSupplier isCancelled) {

// TODO enable fast-path
//        if (n == -1 || n == Long.MAX_VALUE) {
//            for (;;) {
//                if (isCancelled.getAsBoolean()) {
//                    break;
//                }
//                
//                T v = queue.poll();
//                
//                if (v == null) {
//                    actual.onComplete();
//                    break;
//                }
//                
//                actual.onNext(v);
//            }
//            
//            return true;
//        }

        long e = n & COMPLETED_MASK;

        for (; ; ) {

            while (e != n) {
                if (isCancelled.getAsBoolean()) {
                    return true;
                }

                T t = queue.poll();

                if (t == null) {
                    actual.onComplete();
                    return true;
                }

                actual.onNext(t);
                e++;
            }

            if (isCancelled.getAsBoolean()) {
                return true;
            }

            if (queue.isEmpty()) {
                actual.onComplete();
                return true;
            }

            n = field.get(instance);

            if (n == e) {

                n = field.addAndGet(instance, -(e & REQUESTED_MASK));

                if ((n & REQUESTED_MASK) == 0L) {
                    return false;
                }

                e = n & COMPLETED_MASK;
            }
        }

    }

    /**
     * Tries draining the queue if the source just completed.
     *
     * @param actual
     * @param queue
     * @param state
     * @param isCancelled
     */
    public static <T, F> void postComplete(Subscriber<? super T> actual,
                                           Queue<T> queue,
                                           AtomicLongFieldUpdater<F> field,
                                           F instance,
                                           BooleanSupplier isCancelled) {

        if (queue.isEmpty()) {
            actual.onComplete();
            return;
        }

        if (postCompleteDrain(field.get(instance), actual, queue, field, instance, isCancelled)) {
            return;
        }

        for (; ; ) {
            long r = field.get(instance);

            if ((r & COMPLETED_MASK) != 0L) {
                return;
            }

            long u = r | COMPLETED_MASK;
            // (active, r) -> (complete, r) transition
            if (field.compareAndSet(instance, r, u)) {
                // if the requested amount was non-zero, drain the queue
                if (r != 0L) {
                    postCompleteDrain(u, actual, queue, field, instance, isCancelled);
                }

                return;
            }
        }

    }

}
