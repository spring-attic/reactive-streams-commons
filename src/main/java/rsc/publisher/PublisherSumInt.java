package rsc.publisher;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;

import rsc.subscriber.IntReducer;

public final class PublisherSumInt extends PublisherSource<Integer, Integer> {

    public PublisherSumInt(Publisher<? extends Integer> source) {
        super(source);
    }

    @Override
    public void subscribe(Subscriber<? super Integer> s) {
        source.subscribe(new SumIntSubscriber(s));
    }

    static final class SumIntSubscriber extends IntReducer {

        
        public SumIntSubscriber(Subscriber<? super Integer> subscriber) {
            super(subscriber);
        }
        
        @Override
        public void onNext(Integer t) {
            if (!hasValue) {
                hasValue = true;
            }
            accumulator += t.intValue();
        }
    }
}
