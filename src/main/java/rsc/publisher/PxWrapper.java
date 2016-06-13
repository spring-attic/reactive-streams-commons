package rsc.publisher;

import org.reactivestreams.*;

final class PxWrapper<T> extends PublisherSource<T, T> {
    public PxWrapper(Publisher<? extends T> source) {
        super(source);
    }
    
    @Override
    public void subscribe(Subscriber<? super T> s) {
        source.subscribe(s);
    }
}