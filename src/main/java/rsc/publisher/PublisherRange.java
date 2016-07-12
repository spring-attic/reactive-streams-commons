package rsc.publisher;

import java.util.concurrent.atomic.AtomicLongFieldUpdater;

import org.reactivestreams.Subscriber;

import rsc.documentation.BackpressureMode;
import rsc.documentation.BackpressureSupport;
import rsc.documentation.FusionMode;
import rsc.documentation.FusionSupport;
import rsc.flow.*;
import rsc.subscriber.SubscriberState;
import rsc.util.BackpressureHelper;
import rsc.subscriber.EmptySubscription;
import rsc.subscriber.ScalarSubscription;
import rsc.subscriber.SubscriptionHelper;

/**
 * Emits a range of integer values.
 */
@BackpressureSupport(input = BackpressureMode.NOT_APPLICABLE, output = BackpressureMode.BOUNDED)
@FusionSupport(input = { FusionMode.NOT_APPLICABLE }, output = { FusionMode.SYNC, FusionMode.CONDITIONAL })
public final class PublisherRange 
extends Px<Integer>
        implements Fuseable {

    final long start;

    final long end;

    public PublisherRange(int start, int count) {
        if (count < 0) {
            throw new IllegalArgumentException("count >= required but it was " + count);
        }
        long e = (long) start + count;
        if (e - 1 > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("start + count must be less than Integer.MAX_VALUE + 1");
        }

        this.start = start;
        this.end = e;
    }

    @Override
    public void subscribe(Subscriber<? super Integer> s) {
        long st = start;
        long en = end;
        if (st == en) {
            EmptySubscription.complete(s);
            return;
        } else
        if (st + 1 == en) {
            s.onSubscribe(new ScalarSubscription<>(s, (int)st));
            return;
        }
        
        if (s instanceof ConditionalSubscriber) {
            s.onSubscribe(new RangeSubscriptionConditional((ConditionalSubscriber<? super Integer>)s, st, en));
            return;
        }
        s.onSubscribe(new RangeSubscription(s, st, en));
    }

    static final class RangeSubscription
            implements SubscriberState, Producer, SynchronousSubscription<Integer>  {

        final Subscriber<? super Integer> actual;

        final long end;

        volatile boolean cancelled;

        long index;

        volatile long requested;
        static final AtomicLongFieldUpdater<RangeSubscription> REQUESTED =
          AtomicLongFieldUpdater.newUpdater(RangeSubscription.class, "requested");

        public RangeSubscription(Subscriber<? super Integer> actual, long start, long end) {
            this.actual = actual;
            this.index = start;
            this.end = end;
        }

        @Override
        public void request(long n) {
            if (SubscriptionHelper.validate(n)) {
                if (BackpressureHelper.getAndAddCap(REQUESTED, this, n) == 0) {
                    if (n == Long.MAX_VALUE) {
                        fastPath();
                    } else {
                        slowPath(n);
                    }
                }
            }
        }

        @Override
        public void cancel() {
            cancelled = true;
        }

        void fastPath() {
            final long e = end;
            final Subscriber<? super Integer> a = actual;

            for (long i = index; i != e; i++) {
                if (cancelled) {
                    return;
                }

                a.onNext((int) i);
            }

            if (cancelled) {
                return;
            }

            a.onComplete();
        }

        void slowPath(long n) {
            final Subscriber<? super Integer> a = actual;

            long f = end;
            long e = 0;
            long i = index;

            for (; ; ) {

                if (cancelled) {
                    return;
                }

                while (e != n && i != f) {

                    a.onNext((int) i);

                    if (cancelled) {
                        return;
                    }

                    e++;
                    i++;
                }

                if (cancelled) {
                    return;
                }

                if (i == f) {
                    a.onComplete();
                    return;
                }

                n = requested;
                if (n == e) {
                    index = i;
                    n = REQUESTED.addAndGet(this, -e);
                    if (n == 0) {
                        return;
                    }
                    e = 0;
                }
            }
        }

        @Override
        public boolean isCancelled() {
            return cancelled;
        }

        @Override
        public boolean isStarted() {
            return end != index;
        }

        @Override
        public boolean isTerminated() {
            return end == index;
        }

        @Override
        public Object downstream() {
            return actual;
        }

        @Override
        public long requestedFromDownstream() {
            return requested;
        }

        @Override
        public Integer poll() {
            long i = index;
            if (i == end) {
                return null;
            }
            index = i + 1;
            return (int)i;
        }

        @Override
        public boolean isEmpty() {
            return index == end;
        }

        @Override
        public void clear() {
            index = end;
        }
        
        @Override
        public int size() {
            return (int)(end - index);
        }
    }
    
    static final class RangeSubscriptionConditional
            implements SubscriberState, Producer, SynchronousSubscription<Integer> {

        final ConditionalSubscriber<? super Integer> actual;

        final long end;

        volatile boolean cancelled;

        long index;

        volatile long requested;
        static final AtomicLongFieldUpdater<RangeSubscriptionConditional> REQUESTED =
                AtomicLongFieldUpdater.newUpdater(RangeSubscriptionConditional.class, "requested");

        public RangeSubscriptionConditional(ConditionalSubscriber<? super Integer> actual, long start, long end) {
            this.actual = actual;
            this.index = start;
            this.end = end;
        }

        @Override
        public void request(long n) {
            if (SubscriptionHelper.validate(n)) {
                if (BackpressureHelper.getAndAddCap(REQUESTED, this, n) == 0) {
                    if (n == Long.MAX_VALUE) {
                        fastPath();
                    } else {
                        slowPath(n);
                    }
                }
            }
        }

        @Override
        public void cancel() {
            cancelled = true;
        }

        void fastPath() {
            final long e = end;
            final ConditionalSubscriber<? super Integer> a = actual;

            for (long i = index; i != e; i++) {
                if (cancelled) {
                    return;
                }

                a.tryOnNext((int) i);
            }

            if (cancelled) {
                return;
            }

            a.onComplete();
        }

        void slowPath(long n) {
            final ConditionalSubscriber<? super Integer> a = actual;

            long f = end;
            long e = 0;
            long i = index;

            for (; ; ) {

                if (cancelled) {
                    return;
                }

                while (e != n && i != f) {

                    boolean b = a.tryOnNext((int) i);

                    if (cancelled) {
                        return;
                    }

                    if (b) {
                        e++;
                    }
                    i++;
                }

                if (cancelled) {
                    return;
                }

                if (i == f) {
                    a.onComplete();
                    return;
                }

                n = requested;
                if (n == e) {
                    index = i;
                    n = REQUESTED.addAndGet(this, -e);
                    if (n == 0) {
                        return;
                    }
                    e = 0;
                }
            }
        }

        @Override
        public boolean isCancelled() {
            return cancelled;
        }

        @Override
        public boolean isStarted() {
            return end != index;
        }

        @Override
        public boolean isTerminated() {
            return end == index;
        }

        @Override
        public Object downstream() {
            return actual;
        }

        @Override
        public long requestedFromDownstream() {
            return requested;
        }

        @Override
        public Integer poll() {
            long i = index;
            if (i == end) {
                return null;
            }
            index = i + 1;
            return (int)i;
        }

        @Override
        public boolean isEmpty() {
            return index == end;
        }

        @Override
        public void clear() {
            index = end;
        }

        @Override
        public int size() {
            return (int)(end - index);
        }
    }
}
