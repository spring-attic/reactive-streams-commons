package rsc.publisher;

import org.reactivestreams.*;

import rsc.flow.Fuseable;

final class PxFuseableWrapper<T> extends PublisherSource<T, T> 
implements Fuseable {
    public PxFuseableWrapper(Publisher<? extends T> source) {
        super(source);
    }
    
    @Override
    public void subscribe(Subscriber<? super T> s) {
        source.subscribe(s);
    }
}