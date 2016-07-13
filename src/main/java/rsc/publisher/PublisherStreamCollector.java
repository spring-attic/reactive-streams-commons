package rsc.publisher;

import java.util.Objects;
import java.util.function.*;
import java.util.stream.Collector;

import org.reactivestreams.*;

import rsc.documentation.BackpressureMode;
import rsc.documentation.BackpressureSupport;
import rsc.documentation.FusionMode;
import rsc.documentation.FusionSupport;
import rsc.flow.*;
import rsc.subscriber.DeferredScalarSubscriber;

import rsc.subscriber.SubscriptionHelper;
import rsc.util.*;

/**
 * Collects the values from the source sequence into a {@link java.util.stream.Collector}
 * instance.
 *
 * @param <T> the source value type
 * @param <A> an intermediate value type
 * @param <R> the output value type
 */
@BackpressureSupport(input = BackpressureMode.UNBOUNDED, output = BackpressureMode.BOUNDED)
@FusionSupport(input = { FusionMode.NONE }, output = { FusionMode.ASYNC })
public final class PublisherStreamCollector<T, A, R> extends PublisherSource<T, R> implements Fuseable {
    
    final Collector<T, A, R> collector;

    public PublisherStreamCollector(Publisher<? extends T> source, 
            Collector<T, A, R> collector) {
        super(source);
        this.collector = Objects.requireNonNull(collector, "collector");
    }

    @Override
    public long getPrefetch() {
        return Long.MAX_VALUE;
    }

    @Override
    public void subscribe(Subscriber<? super R> s) {
        A container;
        BiConsumer<A, T> accumulator;
        Function<A, R> finisher;

        try {
            container = collector.supplier().get();
            
            accumulator = collector.accumulator();
            
            finisher = collector.finisher();
        } catch (Throwable ex) {
            ExceptionHelper.throwIfFatal(ex);
            SubscriptionHelper.error(s, ex);
            return;
        }
        
        source.subscribe(new PublisherStreamCollectorSubscriber<>(s, container, accumulator, finisher));
    }
    
    static final class PublisherStreamCollectorSubscriber<T, A, R>
    extends DeferredScalarSubscriber<T, R> {
        final BiConsumer<A, T> accumulator;
        
        final Function<A, R> finisher;
        
        A container;
        
        Subscription s;
        
        boolean done;

        public PublisherStreamCollectorSubscriber(Subscriber<? super R> actual,
                A container, BiConsumer<A, T> accumulator, Function<A, R> finisher) {
            super(actual);
            this.container = container;
            this.accumulator = accumulator;
            this.finisher = finisher;
        }
        
        @Override
        public void onSubscribe(Subscription s) {
            if (SubscriptionHelper.validate(this.s, s)) {
                this.s = s;
                
                subscriber.onSubscribe(this);
                
                s.request(Long.MAX_VALUE);
            }
        }
        
        @Override
        public void onNext(T t) {
            if (done) {
                UnsignalledExceptions.onNextDropped(t);
                return;
            }
            try {
                accumulator.accept(container, t);
            } catch (Throwable ex) {
                ExceptionHelper.throwIfFatal(ex);
                s.cancel();
                onError(ex);
                return;
            }
        }
        
        @Override
        public void onError(Throwable t) {
            if (done) {
                UnsignalledExceptions.onErrorDropped(t);
                return;
            }
            done = true;
            container = null;
            subscriber.onError(t);
        }
        
        @Override
        public void onComplete() {
            if (done) {
                return;
            }
            done = true;
            
            A a = container;
            container = null;
            
            R r;
            
            try {
                r = finisher.apply(a);
            } catch (Throwable ex) {
                ExceptionHelper.throwIfFatal(ex);
                subscriber.onError(ex);
                return;
            }
            
            complete(r);
        }
        
        @Override
        public void cancel() {
            super.cancel();
            s.cancel();
        }
    }
}
