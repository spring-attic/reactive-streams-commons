package rsc.publisher;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;

import rsc.subscriber.IntReducer;

public final class PublisherMaxInt extends PublisherSource<Integer, Integer> {

    public PublisherMaxInt(Publisher<? extends Integer> source) {
        super(source);
    }

    @Override
    public void subscribe(Subscriber<? super Integer> s) {
        source.subscribe(new MaxIntSubscriber(s));
    }

    static final class MaxIntSubscriber extends IntReducer {

        public MaxIntSubscriber(Subscriber<? super Integer> subscriber) {
            super(subscriber);
        }
        
        @Override
        public void onNext(Integer t) {
            if (!hasValue) {
                hasValue = true;
                accumulator = t.intValue();
            } else {
                accumulator = Math.max(accumulator, t.intValue());
            }
        }
    }
}
