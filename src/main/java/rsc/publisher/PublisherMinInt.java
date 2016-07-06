package rsc.publisher;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;

import rsc.subscriber.IntReducer;

public final class PublisherMinInt extends PublisherSource<Integer, Integer> {

    public PublisherMinInt(Publisher<? extends Integer> source) {
        super(source);
    }

    @Override
    public void subscribe(Subscriber<? super Integer> s) {
        source.subscribe(new MinIntSubscriber(s));
    }

    static final class MinIntSubscriber extends IntReducer {

        public MinIntSubscriber(Subscriber<? super Integer> subscriber) {
            super(subscriber);
        }
        
        @Override
        public void onNext(Integer t) {
            if (!hasValue) {
                hasValue = true;
                accumulator = t.intValue();
            } else {
                accumulator = Math.min(accumulator, t.intValue());
            }
        }
    }
}
