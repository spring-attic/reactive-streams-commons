package reactivestreams.commons;

import java.util.Iterator;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactivestreams.commons.internal.SingleSubscriptionArbiter;
import reactivestreams.commons.internal.SubscriptionHelper;
import reactivestreams.commons.internal.subscriptions.EmptySubscription;

/**
 * Given a set of source Publishers the values of that Publisher is forwarded to the
 * subscriber which responds first with any signal.
 *
 * @param <T> the value type
 */
public final class PublisherAmb<T> implements Publisher<T> {
    
    final Publisher<? extends T>[] array;
    
    final Iterable<? extends Publisher<? extends T>> iterable;
    
    @SafeVarargs
    public PublisherAmb(Publisher<? extends T>... array) {
        this.array = Objects.requireNonNull(array, "array");
        this.iterable = null;
    }
    
    public PublisherAmb(Iterable<? extends Publisher<? extends T>> iterable) {
        this.array = null;
        this.iterable = Objects.requireNonNull(iterable);
    }
    
    @SuppressWarnings("unchecked")
    @Override
    public void subscribe(Subscriber<? super T> s) {
        Publisher<? extends T>[] a = array;
        int n;
        if (a == null) {
            n = 0;
            a = new Publisher[8];
            
            Iterator<? extends Publisher<? extends T>> it;
            
            try {
                it = iterable.iterator();
            } catch (Throwable e) {
                EmptySubscription.error(s, e);
                return;
            }
            
            if (it == null) {
                EmptySubscription.error(s, new NullPointerException("The iterator returned is null"));
                return;
            }
            
            
            for (;;) {
                
                boolean b;
                
                try {
                    b = it.hasNext();
                } catch (Throwable e) {
                    EmptySubscription.error(s, e);
                    return;
                }
                
                if (!b) {
                    break;
                }
                
                Publisher<? extends T> p;
                
                try {
                    p = it.next();
                } catch (Throwable e) {
                    EmptySubscription.error(s, e);
                    return;
                }
                
                if (p == null) {
                    EmptySubscription.error(s, new NullPointerException("The Publisher returned by the iterator is null"));
                    return;
                }
                
                if (n == a.length) {
                    Publisher<? extends T>[] c = new Publisher[n + (n >> 2)];
                    System.arraycopy(a, 0, c, 0, n);
                    a = c;
                }
                a[n++] = p;
            }
            
        } else {
            n = a.length;
        }
        
        if (n == 0) {
            EmptySubscription.complete(s);
            return;
        }
        if (n == 1) {
            Publisher<? extends T> p = a[0];
            
            if (p == null) {
                EmptySubscription.error(s, new NullPointerException("The single source Publisher is null"));
            } else {
                p.subscribe(s);
            }
            return;
        }
        
        PublisherAmbCoordinator<T> coordinator = new PublisherAmbCoordinator<>(n);
        
        coordinator.subscribe(a, n, s);
    }
    
    static final class PublisherAmbCoordinator<T>
    extends AtomicInteger
    implements Subscription {

        /** */
        private static final long serialVersionUID = 5137266795390425172L;

        final PublisherAmbSubscriber<T>[] subscribers;
        
        volatile boolean cancelled;
        
        @SuppressWarnings("unchecked")
        public PublisherAmbCoordinator(int n) {
            super(Integer.MIN_VALUE);
            subscribers = new PublisherAmbSubscriber[n];
        }
        
        void subscribe(Publisher<? extends T>[] sources, int n, Subscriber<? super T> actual) {
            PublisherAmbSubscriber<T>[] a = subscribers;
            
            for (int i = 0; i < n; i++) {
                a[i] = new PublisherAmbSubscriber<>(actual, this, i);
            }
            
            actual.onSubscribe(this);
            
            for (int i = 0; i < n; i++) {
                if (cancelled || get() != Integer.MIN_VALUE) {
                    return;
                }
                
                Publisher<? extends T> p = sources[i];

                if (p == null) {
                    if (compareAndSet(Integer.MIN_VALUE, -1)) {
                        actual.onError(new NullPointerException("The " + i + " th Publisher source is null"));
                    }
                    return;
                }
                
                p.subscribe(a[i]);
            }
            
        }
        
        @Override
        public void request(long n) {
            if (SubscriptionHelper.validate(n)) {
                int w = get();
                if (w >= 0) {
                    subscribers[w].arbiter.request(n);
                } else {
                    for (PublisherAmbSubscriber<T> s : subscribers) {
                        s.arbiter.request(n);
                    }
                }
            }
        }

        @Override
        public void cancel() {
            if (cancelled) {
                return;
            }
            cancelled = true;
            
            int w = get();
            if (w >= 0) {
                subscribers[w].arbiter.cancel();
            } else {
                for (PublisherAmbSubscriber<T> s : subscribers) {
                    s.arbiter.cancel();
                }
            }
        }
        
        boolean tryWin(int index) {
            if (get() == Integer.MIN_VALUE) {
                if (compareAndSet(Integer.MIN_VALUE, index)) {
                    
                    PublisherAmbSubscriber<T>[] a = subscribers;
                    int n = a.length;
                    
                    for (int i = 0; i < n; i++) {
                        if (i != index) {
                            a[i].arbiter.cancel();
                        }
                    }
                    
                    return true;
                }
            }
            return false;
        }
    }
    
    static final class PublisherAmbSubscriber<T> implements Subscriber<T> {
        final Subscriber<? super T> actual;
        
        final PublisherAmbCoordinator<T> parent;
        
        final int index;
        
        final SingleSubscriptionArbiter arbiter;
        
        boolean won;
        
        public PublisherAmbSubscriber(Subscriber<? super T> actual, PublisherAmbCoordinator<T> parent, int index) {
            this.actual = actual;
            this.parent = parent;
            this.index = index;
            this.arbiter = new SingleSubscriptionArbiter();
        }

        @Override
        public void onSubscribe(Subscription s) {
            arbiter.set(s);
        }

        @Override
        public void onNext(T t) {
            if (won) {
                actual.onNext(t);
            } else
            if (parent.tryWin(index)) {
                won = true;
                actual.onNext(t);
            }
        }

        @Override
        public void onError(Throwable t) {
            if (won) {
                actual.onError(t);
            } else
            if (parent.tryWin(index)) {
                won = true;
                actual.onError(t);
            }
        }

        @Override
        public void onComplete() {
            if (won) {
                actual.onComplete();
            } else
            if (parent.tryWin(index)) {
                won = true;
                actual.onComplete();
            }
        }
    }
}
