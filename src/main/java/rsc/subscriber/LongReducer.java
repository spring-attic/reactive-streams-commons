package rsc.subscriber;

import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

/**
 * Base class for reducing long values (without the constant re-boxing).
 * <p>
 * Use the {@code accumulator} field and set the {@code hasValue} to indicate
 * there is actually something in the accumulator.
 */
public abstract class LongReducer extends DeferredScalarSubscriber<Long, Long> {
        protected long accumulator;
        
        protected boolean hasValue;
        
        protected Subscription s;
        
        public LongReducer(Subscriber<? super Long> subscriber) {
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