package rsc.publisher;

import java.util.Arrays;
import java.util.Iterator;
import java.util.Objects;
import java.util.concurrent.atomic.*;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import rsc.flow.MultiReceiver;

import rsc.subscriber.MultiSubscriptionSubscriber;
import rsc.subscriber.SubscriptionHelper;
import rsc.util.*;

/**
 * Concatenates a fixed array of Publishers' values.
 *
 * @param <T> the value type
 */
public final class PublisherConcatArray<T> 
extends Px<T>
        implements MultiReceiver {

    final Publisher<? extends T>[] array;
    
    final boolean delayError;

    @SafeVarargs
    public PublisherConcatArray(boolean delayError, Publisher<? extends T>... array) {
        this.array = Objects.requireNonNull(array, "array");
        this.delayError = delayError;
    }

    @Override
    public Iterator<?> upstreams() {
        return Arrays.asList(array).iterator();
    }

    @Override
    public long upstreamCount() {
        return array.length;
    }

    @Override
    public void subscribe(Subscriber<? super T> s) {
        Publisher<? extends T>[] a = array;

        if (a.length == 0) {
            SubscriptionHelper.complete(s);
            return;
        }
        if (a.length == 1) {
            Publisher<? extends T> p = a[0];

            if (p == null) {
                SubscriptionHelper.error(s, new NullPointerException("The single source Publisher is null"));
            } else {
                p.subscribe(s);
            }
            return;
        }

        if (delayError) {
            PublisherConcatArrayDelayErrorSubscriber<T> parent = new PublisherConcatArrayDelayErrorSubscriber<>(s, a);

            s.onSubscribe(parent);

            if (!parent.isCancelled()) {
                parent.onComplete();
            }
            return;
        }
        PublisherConcatArraySubscriber<T> parent = new PublisherConcatArraySubscriber<>(s, a);

        s.onSubscribe(parent);

        if (!parent.isCancelled()) {
            parent.onComplete();
        }
    }

    /**
     * Returns a new instance which has the additional source to be merged together with
     * the current array of sources.
     * <p>
     * This operation doesn't change the current PublisherMerge instance.
     * 
     * @param source the new source to merge with the others
     * @return the new PublisherConcatArray instance
     */
    public PublisherConcatArray<T> concatAdditionalSourceLast(Publisher<? extends T> source) {
        int n = array.length;
        @SuppressWarnings("unchecked")
        Publisher<? extends T>[] newArray = new Publisher[n + 1];
        System.arraycopy(array, 0, newArray, 0, n);
        newArray[n] = source;
        
        return new PublisherConcatArray<>(delayError, newArray);
    }

    /**
     * Returns a new instance which has the additional first source to be concatenated together with
     * the current array of sources.
     * <p>
     * This operation doesn't change the current PublisherConcatArray instance.
     * 
     * @param source the new source to merge with the others
     * @return the new PublisherConcatArray instance
     */
    public PublisherConcatArray<T> concatAdditionalSourceFirst(Publisher<? extends T> source) {
        int n = array.length;
        @SuppressWarnings("unchecked")
        Publisher<? extends T>[] newArray = new Publisher[n + 1];
        System.arraycopy(array, 0, newArray, 1, n);
        newArray[0] = source;
        
        return new PublisherConcatArray<>(delayError, newArray);
    }

    
    static final class PublisherConcatArraySubscriber<T>
            extends MultiSubscriptionSubscriber<T, T> {

        final Publisher<? extends T>[] sources;

        int index;

        volatile int wip;
        @SuppressWarnings("rawtypes")
        static final AtomicIntegerFieldUpdater<PublisherConcatArraySubscriber> WIP =
          AtomicIntegerFieldUpdater.newUpdater(PublisherConcatArraySubscriber.class, "wip");

        long produced;

        public PublisherConcatArraySubscriber(Subscriber<? super T> actual, Publisher<? extends T>[] sources) {
            super(actual);
            this.sources = sources;
        }

        @Override
        public void onNext(T t) {
            produced++;

            subscriber.onNext(t);
        }

        @Override
        public void onComplete() {
            if (WIP.getAndIncrement(this) == 0) {
                Publisher<? extends T>[] a = sources;
                do {

                    if (isCancelled()) {
                        return;
                    }

                    int i = index;
                    if (i == a.length) {
                        subscriber.onComplete();
                        return;
                    }

                    Publisher<? extends T> p = a[i];

                    if (p == null) {
                        subscriber.onError(new NullPointerException("The " + i + "th source Publisher is null"));
                        return;
                    }

                    long c = produced;
                    if (c != 0L) {
                        produced = 0L;
                        produced(c);
                    }
                    p.subscribe(this);

                    if (isCancelled()) {
                        return;
                    }

                    index = ++i;
                } while (WIP.decrementAndGet(this) != 0);
            }

        }
    }

    static final class PublisherConcatArrayDelayErrorSubscriber<T>
    extends MultiSubscriptionSubscriber<T, T> {

        final Publisher<? extends T>[] sources;

        int index;

        volatile int wip;
        @SuppressWarnings("rawtypes")
        static final AtomicIntegerFieldUpdater<PublisherConcatArrayDelayErrorSubscriber> WIP =
        AtomicIntegerFieldUpdater.newUpdater(PublisherConcatArrayDelayErrorSubscriber.class, "wip");

        volatile Throwable error;
        @SuppressWarnings("rawtypes")
        static final AtomicReferenceFieldUpdater<PublisherConcatArrayDelayErrorSubscriber, Throwable> ERROR =
                AtomicReferenceFieldUpdater.newUpdater(PublisherConcatArrayDelayErrorSubscriber.class, Throwable.class, "error");
        
        long produced;

        public PublisherConcatArrayDelayErrorSubscriber(Subscriber<? super T> actual, Publisher<? extends T>[] sources) {
            super(actual);
            this.sources = sources;
        }

        @Override
        public void onNext(T t) {
            produced++;

            subscriber.onNext(t);
        }
        
        @Override
        public void onError(Throwable t) {
            if (ExceptionHelper.addThrowable(ERROR, this, t)) {
                onComplete();
            } else {
                UnsignalledExceptions.onErrorDropped(t);
            }
        }

        @Override
        public void onComplete() {
            if (WIP.getAndIncrement(this) == 0) {
                Publisher<? extends T>[] a = sources;
                do {

                    if (isCancelled()) {
                        return;
                    }

                    int i = index;
                    if (i == a.length) {
                        Throwable e = ExceptionHelper.terminate(ERROR, this);
                        if (e != null) {
                            subscriber.onError(e);
                        } else {
                            subscriber.onComplete();
                        }
                        return;
                    }

                    Publisher<? extends T> p = a[i];

                    if (p == null) {
                        subscriber.onError(new NullPointerException("The " + i + "th source Publisher is null"));
                        return;
                    }

                    long c = produced;
                    if (c != 0L) {
                        produced = 0L;
                        produced(c);
                    }
                    p.subscribe(this);

                    if (isCancelled()) {
                        return;
                    }

                    index = ++i;
                } while (WIP.decrementAndGet(this) != 0);
            }

        }
    }

}
