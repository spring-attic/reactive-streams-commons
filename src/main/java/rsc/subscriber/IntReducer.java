package rsc.subscriber;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

/**
 * Base class for reducing integer values (without the constant re-boxing).
 * <p>
 * Use the {@code accumulator} field and set the {@code hasValue} to indicate
 * there is actually something in the accumulator.
 */
public abstract class IntReducer extends DeferredScalarSubscriber<Integer, Integer> {
        protected int accumulator;
        
        protected boolean hasValue;
        
        protected Subscription s;
        
        public IntReducer(Subscriber<? super Integer> subscriber) {
            super(subscriber);
        }
        
        @Override
        public final void onSubscribe(Subscription s) {
            this.s = s;
            
            subscriber.onSubscribe(this);
            
            s.request(Long.MAX_VALUE);
        }
        
        @Override
        public final void onComplete() {
            if (hasValue) {
                complete(accumulator);
            } else {
                super.onComplete();
            }
        }
        
        @Override
        public final void cancel() {
            super.cancel();
            s.cancel();
        }
    }