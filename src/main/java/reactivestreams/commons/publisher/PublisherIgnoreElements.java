/*
 * Copyright (c) 2011-2016 Pivotal Software Inc, All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package reactivestreams.commons.publisher;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactivestreams.commons.graph.Publishable;
import reactivestreams.commons.graph.Subscribable;
import reactivestreams.commons.util.SubscriptionHelper;

/**
 * Ignores normal values and passes only the terminal signals along.
 *
 * @param <T> the value type
 */
public final class PublisherIgnoreElements<T> extends PublisherSource<T, T> {

    public PublisherIgnoreElements(Publisher<? extends T> source) {
        super(source);
    }
    
    @Override
    public void subscribe(Subscriber<? super T> s) {
        source.subscribe(new PublisherIgnoreElementsSubscriber<>(s));
    }
    
    static final class PublisherIgnoreElementsSubscriber<T> implements Subscriber<T>, Subscribable, Subscription,
                                                                       Publishable {
        final Subscriber<? super T> actual;
        
        Subscription s;
        
        public PublisherIgnoreElementsSubscriber(Subscriber<? super T> actual) {
            this.actual = actual;
        }

        @Override
        public void onSubscribe(Subscription s) {
            if (SubscriptionHelper.validate(this.s, s)) {
                this.s = s;
                
                actual.onSubscribe(this);
                
                s.request(Long.MAX_VALUE);
            }
        }
        
        @Override
        public void onNext(T t) {
            // deliberately ignored
        }
        
        @Override
        public void onError(Throwable t) {
            actual.onError(t);
        }
        
        @Override
        public void onComplete() {
            actual.onComplete();
        }

        @Override
        public Object downstream() {
            return actual;
        }
        
        @Override
        public void request(long n) {
            // requests Long.MAX_VALUE anyway
        }
        
        @Override
        public void cancel() {
            s.cancel();
        }
        
        @Override
        public Object upstream() {
            return s;
        }
    }
}
