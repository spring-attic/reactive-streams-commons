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

import reactivestreams.commons.flow.Fuseable;
import reactivestreams.commons.state.Completable;
import reactivestreams.commons.util.EmptySubscription;

/**
 * Represents an empty publisher which only calls onSubscribe and onComplete.
 * <p>
 * This Publisher is effectively stateless and only a single instance exists.
 * Use the {@link #instance()} method to obtain a properly type-parametrized view of it.
 */
public final class PublisherEmpty 
extends PublisherBase<Object>
implements Fuseable.ScalarSupplier<Object>, Completable {

    private static final Publisher<Object> INSTANCE = new PublisherEmpty();

    private PublisherEmpty() {
        // deliberately no op
    }

    @Override
    public void subscribe(Subscriber<? super Object> s) {
        s.onSubscribe(EmptySubscription.INSTANCE);
        s.onComplete();
    }

    /**
     * Returns a properly parametrized instance of this empty Publisher.
     *
     * @param <T> the output type
     * @return a properly parametrized instance of this empty Publisher
     */
    @SuppressWarnings("unchecked")
    public static <T> PublisherBase<T> instance() {
        return (PublisherBase<T>) INSTANCE;
    }

    @Override
    public Object get() {
        return null; /* Scalar optimizations on empty */
    }

    @Override
    public boolean isStarted() {
        return false;
    }

    @Override
    public boolean isTerminated() {
        return true;
    }
}
