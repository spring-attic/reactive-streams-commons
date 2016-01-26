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
import reactivestreams.commons.subscriber.SubscriberDeferredScalar;
import reactivestreams.commons.util.SubscriptionHelper;

public final class PublisherIsEmpty<T> extends PublisherSource<T, Boolean> {

    public PublisherIsEmpty(Publisher<? extends T> source) {
        super(source);
    }

    @Override
    public void subscribe(Subscriber<? super Boolean> s) {
        source.subscribe(new PublisherIsEmptySubscriber<>(s));
    }

    static final class PublisherIsEmptySubscriber<T> extends SubscriberDeferredScalar<T, Boolean>
            implements Publishable {
        Subscription s;

        public PublisherIsEmptySubscriber(Subscriber<? super Boolean> actual) {
            super(actual);
        }

        @Override
        public void cancel() {
            super.cancel();
            s.cancel();
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
            s.cancel();

            complete(false);
        }

        @Override
        public void onComplete() {
            complete(true);
        }

        @Override
        public Object upstream() {
            return s;
        }
    }
}
