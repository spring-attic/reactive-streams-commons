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
package reactivestreams.commons.internal;

import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

import org.reactivestreams.Subscriber;
import reactivestreams.commons.internal.subscription.ScalarDelayedSubscriptionTrait;

public final class ScalarDelayedArbiter<T> implements ScalarDelayedSubscriptionTrait<T> {

    final Subscriber<? super T> actual;
    
    T value;

    volatile int state;
    @SuppressWarnings("rawtypes")
    static final AtomicIntegerFieldUpdater<ScalarDelayedArbiter> STATE =
            AtomicIntegerFieldUpdater.newUpdater(ScalarDelayedArbiter.class, "state");

    public ScalarDelayedArbiter(Subscriber<? super T> actual) {
        this.actual = actual;
    }

    @Override
    public int sdsGetState() {
        return state;
    }

    @Override
    public void sdsSetState(int updated) {
        state = updated;
    }

    @Override
    public boolean sdsCasState(int expected, int updated) {
        return STATE.compareAndSet(this, expected, updated);
    }

    @Override
    public T sdsGetValue() {
        return value;
    }

    @Override
    public void sdsSetValue(T value) {
        this.value = value;
    }

    @Override
    public Subscriber<? super T> sdsGetSubscriber() {
        return actual;
    }
    
    public void set(T value) {
        sdsSet(value);
    }
}
