package reactivestreams.commons.publisher;

import java.util.*;
import java.util.concurrent.atomic.*;
import java.util.function.Consumer;

import org.reactivestreams.*;

import reactivestreams.commons.flow.*;
import reactivestreams.commons.state.Cancellable;
import reactivestreams.commons.util.*;

/**
 * Connects to the underlying ConnectablePublisher once the given number of Subscribers subscribed
 * to it and disconnects once all Subscribers cancelled their Subscriptions.
 *
 * @param <T> the value type
 */
public final class ConnectablePublisherRefCount<T> extends Px<T>
        implements Receiver, Loopback {
    
    final ConnectablePublisher<? extends T> source;
    
    final int n;

    volatile State<T> connection;
    @SuppressWarnings("rawtypes")
    static final AtomicReferenceFieldUpdater<ConnectablePublisherRefCount, State> CONNECTION =
            AtomicReferenceFieldUpdater.newUpdater(ConnectablePublisherRefCount.class, State.class, "connection");
    
    public ConnectablePublisherRefCount(ConnectablePublisher<? extends T> source, int n) {
        if (n <= 0) {
            throw new IllegalArgumentException("n > 0 required but it was " + n);
        }
        this.source = Objects.requireNonNull(source, "source");
        this.n = n;
    }
    
    @Override
    public void subscribe(Subscriber<? super T> s) {
        State<T> state;
        
        for (;;) {
            state = connection;
            if (state == null || state.isDisconnected()) {
                State<T> u = new State<>(n, this);
                
                if (!CONNECTION.compareAndSet(this, state, u)) {
                    continue;
                }
                
                state = u;
            }
            
            state.subscribe(s);
            break;
        }
    }


    @Override
    public Object connectedOutput() {
        return connection;
    }

    @Override
    public Object upstream() {
        return source;
    }

    static final class State<T> implements Consumer<Cancellable>, MultiProducer, Receiver {
        
        final int n;
        
        final ConnectablePublisherRefCount<? extends T> parent;
        
        volatile int subscribers;
        @SuppressWarnings("rawtypes")
        static final AtomicIntegerFieldUpdater<State> SUBSCRIBERS =
                AtomicIntegerFieldUpdater.newUpdater(State.class, "subscribers");
        
        volatile Cancellable disconnect;
        @SuppressWarnings("rawtypes")
        static final AtomicReferenceFieldUpdater<State, Cancellable> DISCONNECT =
                AtomicReferenceFieldUpdater.newUpdater(State.class, Cancellable.class, "disconnect");
        
        static final Cancellable DISCONNECTED = new EmptyCancellable();

        public State(int n, ConnectablePublisherRefCount<? extends T> parent) {
            this.n = n;
            this.parent = parent;
        }
        
        void subscribe(Subscriber<? super T> s) {
            // FIXME think about what happens when subscribers come and go below the connection threshold concurrently
            
            InnerSubscriber<T> inner = new InnerSubscriber<>(s, this);
            parent.source.subscribe(inner);
            
            if (SUBSCRIBERS.incrementAndGet(this) == n) {
                parent.source.connect(this);
            }
        }
        
        @Override
        public void accept(Cancellable r) {
            if (!DISCONNECT.compareAndSet(this, null, r)) {
                r.cancel();
            }
        }
        
        void doDisconnect() {
            Cancellable a = disconnect;
            if (a != DISCONNECTED) {
                a = DISCONNECT.getAndSet(this, DISCONNECTED);
                if (a != null && a != DISCONNECTED) {
                    a.cancel();
                }
            }
        }
        
        boolean isDisconnected() {
            return disconnect == DISCONNECTED;
        }
        
        void innerCancelled() {
            if (SUBSCRIBERS.decrementAndGet(this) == 0) {
                doDisconnect();
            }
        }
        
        void upstreamFinished() {
            Cancellable a = disconnect;
            if (a != DISCONNECTED) {
                DISCONNECT.getAndSet(this, DISCONNECTED);
            }
        }

        @Override
        public Iterator<?> downstreams() {
            return null;
        }

        @Override
        public long downstreamCount() {
            return subscribers;
        }

        @Override
        public Object upstream() {
            return parent;
        }

        static final class InnerSubscriber<T> implements Subscriber<T>, Subscription, Receiver, Producer, Loopback {

            final Subscriber<? super T> actual;
            
            final State<T> parent;
            
            Subscription s;
            
            public InnerSubscriber(Subscriber<? super T> actual, State<T> parent) {
                this.actual = actual;
                this.parent = parent;
            }

            @Override
            public void onSubscribe(Subscription s) {
                if (SubscriptionHelper.validate(this.s, s)) {
                    this.s = s;
                    actual.onSubscribe(this);
                }
            }

            @Override
            public void onNext(T t) {
                actual.onNext(t);
            }

            @Override
            public void onError(Throwable t) {
                actual.onError(t);
                parent.upstreamFinished();
            }

            @Override
            public void onComplete() {
                actual.onComplete();
                parent.upstreamFinished();
            }
            
            @Override
            public void request(long n) {
                s.request(n);
            }
            
            @Override
            public void cancel() {
                s.cancel();
                parent.innerCancelled();
            }

            @Override
            public Object downstream() {
                return actual;
            }

            @Override
            public Object upstream() {
                return s;
            }

            @Override
            public Object connectedInput() {
                return null;
            }

            @Override
            public Object connectedOutput() {
                return s;
            }
        }
    }
}
