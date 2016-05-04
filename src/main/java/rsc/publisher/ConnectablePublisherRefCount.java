package rsc.publisher;

import java.util.*;
import java.util.concurrent.atomic.*;
import java.util.function.Consumer;

import org.reactivestreams.*;

import rsc.documentation.Operator;
import rsc.documentation.OperatorType;
import rsc.flow.Cancellation;
import rsc.flow.MultiProducer;
import rsc.flow.Receiver;
import rsc.util.SubscriptionHelper;
import rsc.flow.Loopback;
import rsc.flow.Producer;

/**
 * Connects to the underlying ConnectablePublisher once the given number of Subscribers subscribed
 * to it and disconnects once all Subscribers cancelled their Subscriptions.
 *
 * @param <T> the value type
 */
@Operator(traits = OperatorType.CONNECTABLE, aliases = {"refCount"})
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

    static final class State<T> implements Consumer<Cancellation>, MultiProducer, Receiver {
        
        final int n;
        
        final ConnectablePublisherRefCount<? extends T> parent;
        
        volatile int subscribers;
        @SuppressWarnings("rawtypes")
        static final AtomicIntegerFieldUpdater<State> SUBSCRIBERS =
                AtomicIntegerFieldUpdater.newUpdater(State.class, "subscribers");
        
        volatile Cancellation disconnect;
        @SuppressWarnings("rawtypes")
        static final AtomicReferenceFieldUpdater<State, Cancellation> DISCONNECT =
                AtomicReferenceFieldUpdater.newUpdater(State.class, Cancellation.class, "disconnect");
        
        static final Cancellation DISCONNECTED = () -> { };

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
        public void accept(Cancellation r) {
            if (!DISCONNECT.compareAndSet(this, null, r)) {
                r.dispose();
            }
        }
        
        void doDisconnect() {
            Cancellation a = disconnect;
            if (a != DISCONNECTED) {
                a = DISCONNECT.getAndSet(this, DISCONNECTED);
                if (a != null && a != DISCONNECTED) {
                    a.dispose();
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
            Cancellation a = disconnect;
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
