package rsc.publisher;

import java.util.concurrent.atomic.AtomicLongFieldUpdater;

import org.reactivestreams.Subscriber;

import rsc.flow.Fuseable;
import rsc.util.BackpressureHelper;
import rsc.util.SubscriptionHelper;

/**
 * Streams the characters of a string.
 */
public final class PublisherCharSequence extends Px<Integer> {

    final CharSequence string;

    public PublisherCharSequence(CharSequence string) {
        this.string = string;
    }

    @Override
    public void subscribe(Subscriber<? super Integer> s) {
        s.onSubscribe(new CharSequenceSubscription(s, string));
    }
    
    static final class CharSequenceSubscription 
    implements Fuseable.QueueSubscription<Integer> {
        final Subscriber<? super Integer> actual;
        
        final CharSequence string;
        
        final int end;
        
        int index;
        
        volatile boolean cancelled;
        
        volatile long requested;
        static final AtomicLongFieldUpdater<CharSequenceSubscription> REQUESTED =
                AtomicLongFieldUpdater.newUpdater(CharSequenceSubscription.class, "requested");
        
        public CharSequenceSubscription(Subscriber<? super Integer> actual, CharSequence string) {
            this.actual = actual;
            this.string = string;
            this.end = string.length();
        }
        
        @Override
        public void cancel() {
            cancelled = true;
        }
        
        @Override
        public void request(long n) {
            if (SubscriptionHelper.validate(n)) {
                if (BackpressureHelper.getAndAddCap(REQUESTED, this, n) == 0) {
                    if (n == Long.MAX_VALUE) {
                        fastPath();
                    } else {
                        slowPath(n);
                    }
                }
            }
        }
        
        void fastPath() {
            int e = end;
            CharSequence s = string;
            Subscriber<? super Integer> a = actual;
            
            for (int i = index; i != e; i++) {
                if (cancelled) {
                    return;
                }
                
                a.onNext((int)s.charAt(i));
            }
            
            if (!cancelled) {
                a.onComplete();
            }
        }
        
        void slowPath(long r) {
            long e = 0L;
            int i = index;
            int f = end;
            CharSequence s = string;
            Subscriber<? super Integer> a = actual;
            
            for (;;) {
                
                while (e != r && i != f) {
                    if (cancelled) {
                        return;
                    }
                    
                    a.onNext((int)s.charAt(i));
                    
                    i++;
                    e++;
                }
                
                if (i == f) {
                    if (!cancelled) {
                        a.onComplete();
                    }
                    return;
                }
                
                r = requested;
                if (e == r) {
                    index = i;
                    r = REQUESTED.addAndGet(this, -e);
                    if (r == 0L) {
                        break;
                    }
                }
            }
        }
        
        @Override
        public int requestFusion(int requestedMode) {
            return requestedMode & Fuseable.SYNC;
        }
        
        @Override
        public Integer poll() {
            int i = index;
            if (i != end) {
                index = i + 1;
                return (int)string.charAt(i);
            }
            return null;
        }
        
        @Override
        public boolean isEmpty() {
            return index != end;
        }
        
        @Override
        public int size() {
            return end - index;
        }
        
        @Override
        public void clear() {
            index = end;
        }
    }

}
