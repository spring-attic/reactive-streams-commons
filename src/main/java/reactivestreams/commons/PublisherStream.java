package reactivestreams.commons;

import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

import org.reactivestreams.*;

import reactivestreams.commons.internal.*;
import reactivestreams.commons.internal.subscriptions.EmptySubscription;

/**
 * Emits the contents of a Stream source.
 *
 * @param <T> the value type
 */
public final class PublisherStream<T> implements Publisher<T> {

    final Stream<? extends T> stream;
    
    public PublisherStream(Stream<? extends T> iterable) {
        this.stream = Objects.requireNonNull(iterable, "stream");
    }
    
    @Override
    public void subscribe(Subscriber<? super T> s) {
        Iterator<? extends T> it;
        
        try {
            it = stream.iterator();
        } catch (Throwable e) {
            EmptySubscription.error(s, e);
            return;
        }

        subscribe(s, it);
    }
    
    /**
     * Common method to take an Iterator as a source of values.
     * @param s
     * @param it
     */
    static <T> void subscribe(Subscriber<? super T> s, Iterator<? extends T> it) {
        if (it == null) {
            EmptySubscription.error(s, new NullPointerException("The iterator is null"));
            return;
        }
        
        boolean b;
        
        try {
            b = it.hasNext();
        } catch (Throwable e) {
            EmptySubscription.error(s, e);
            return;
        }
        if (!b) {
            EmptySubscription.complete(s);
            return;
        }
        
        s.onSubscribe(new PublisherIterableSubscription<>(s, it));
    }
    
    static final class PublisherIterableSubscription<T>
    extends AtomicLong
    implements Subscription {
        /** */
        private static final long serialVersionUID = 5781366097814716112L;
        
        final Subscriber<? super T> actual;

        final Iterator<? extends T> iterator;
        
        volatile boolean cancelled;
        
        public PublisherIterableSubscription(Subscriber<? super T> actual, Iterator<? extends T> iterator) {
            this.actual = actual;
            this.iterator = iterator;
        }

        @Override
        public void request(long n) {
            if (SubscriptionHelper.validate(n)) {
                if (BackpressureHelper.add(this, n) == 0) {
                    if (n == Long.MAX_VALUE) {
                        fastPath();
                    } else {
                        slowPath(n);
                    }
                }
            }
        }

        void slowPath(long n) {
            final Iterator<? extends T> a = iterator;
            final Subscriber<? super T> s = actual;
            
            long e = 0L;
            
            for (;;) {
                
                while (e != n) {
                    T t;
                    
                    try {
                        t = a.next();
                    } catch (Throwable ex) {
                        s.onError(ex);
                        return;
                    }
                    
                    if (cancelled) {
                        return;
                    }

                    if (t == null) {
                        s.onError(new NullPointerException("The iterator returned a null value"));
                        return;
                    }

                    s.onNext(t);
                    
                    if (cancelled) {
                        return;
                    }
                    
                    boolean b;
                    
                    try {
                        b = a.hasNext();
                    } catch (Throwable ex) {
                        s.onError(ex);
                        return;
                    }
                    
                    if (cancelled) {
                        return;
                    }

                    if (!b) {
                        s.onComplete();
                        return;
                    }
                    
                    e++;
                }
                
                n = get() - e;
                
                if (n == 0) {
                    n = addAndGet(-e);
                    if (n == 0L) {
                        return;
                    }
                    e = 0L;
                }
            }
        }
        
        void fastPath() {
            final Iterator<? extends T> a = iterator;
            final Subscriber<? super T> s = actual;

            for (;;) {
                
                if (cancelled) {
                    return;
                }
                
                T t;
                
                try {
                    t = a.next();
                } catch (Exception ex) {
                    s.onError(ex);
                    return;
                }

                if (cancelled) {
                    return;
                }

                if (t == null) {
                    s.onError(new NullPointerException("The iterator returned a null value"));
                    return;
                }
                
                s.onNext(t);

                if (cancelled) {
                    return;
                }

                boolean b;
                
                try {
                    b = a.hasNext();
                } catch (Exception ex) {
                    s.onError(ex);
                    return;
                }

                if (cancelled) {
                    return;
                }

                if (!b) {
                    s.onComplete();
                    return;
                }
            }
        }
        
        @Override
        public void cancel() {
            cancelled = true;
        }
    }
}
