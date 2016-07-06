package rsc.publisher;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;

import rsc.subscriber.LongReducer;

public final class PublisherSumLong extends PublisherSource<Long, Long> {

    public PublisherSumLong(Publisher<? extends Long> source) {
        super(source);
    }

    @Override
    public void subscribe(Subscriber<? super Long> s) {
        source.subscribe(new SumLongSubscriber(s));
    }

    static final class SumLongSubscriber extends LongReducer {

        public SumLongSubscriber(Subscriber<? super Long> subscriber) {
            super(subscriber);
        }
        
        @Override
        public void onNext(Long t) {
            if (!hasValue) {
                hasValue = true;
            }
            accumulator += t.longValue();
        }
    }
}
