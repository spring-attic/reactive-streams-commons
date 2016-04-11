package reactivestreams.commons.publisher;

import java.util.concurrent.atomic.*;

import org.junit.*;
import org.reactivestreams.*;

import reactivestreams.commons.processor.SimpleProcessor;

public class PublisherAwaitOnSubscribeTest {

    @Test
    public void requestDelayed() {
        AtomicBoolean state = new AtomicBoolean();
        AtomicReference<Throwable> e = new AtomicReference<>();
        
        Px.just(1).awaitOnSubscribe()
        .subscribe(new Subscriber<Integer>() {
            boolean open;
            @Override
            public void onSubscribe(Subscription s) {
                s.request(1);
                open = true;
            }
            
            @Override
            public void onNext(Integer t) {
                state.set(open);
            }
            
            @Override
            public void onError(Throwable t) {
                e.set(t);
            }
            
            @Override
            public void onComplete() {
                
            }
        });
        
        Assert.assertNull("Error: " + e.get(), e.get());
        
        Assert.assertTrue("Not open!", state.get());
    }

    @Test
    public void cancelDelayed() {
        AtomicBoolean state1 = new AtomicBoolean();
        AtomicBoolean state2 = new AtomicBoolean();
        AtomicReference<Throwable> e = new AtomicReference<>();
        
        SimpleProcessor<Integer> sp = new SimpleProcessor<>();
        
        sp.awaitOnSubscribe()
        .doOnCancel(() -> state2.set(state1.get()))
        .subscribe(new Subscriber<Integer>() {
            @Override
            public void onSubscribe(Subscription s) {
                s.cancel();
                state1.set(true);
            }
            
            @Override
            public void onNext(Integer t) {
            }
            
            @Override
            public void onError(Throwable t) {
                e.set(t);
            }
            
            @Override
            public void onComplete() {
                
            }
        });
        
        Assert.assertNull("Error: " + e.get(), e.get());
        
        Assert.assertFalse("Cancel executed before onSubscribe finished", state2.get());
        Assert.assertFalse("Has subscribers?!", sp.hasSubscribers());
    }

    @Test
    public void requestNotDelayed() {
        AtomicBoolean state = new AtomicBoolean();
        AtomicReference<Throwable> e = new AtomicReference<>();
        
        Px.just(1)
        .subscribe(new Subscriber<Integer>() {
            boolean open;
            @Override
            public void onSubscribe(Subscription s) {
                s.request(1);
                open = true;
            }
            
            @Override
            public void onNext(Integer t) {
                state.set(open);
            }
            
            @Override
            public void onError(Throwable t) {
                e.set(t);
            }
            
            @Override
            public void onComplete() {
                
            }
        });
        
        Assert.assertNull("Error: " + e.get(), e.get());
        
        Assert.assertFalse("Request delayed!", state.get());
    }

    @Test
    public void cancelNotDelayed() {
        AtomicBoolean state1 = new AtomicBoolean();
        AtomicBoolean state2 = new AtomicBoolean();
        AtomicReference<Throwable> e = new AtomicReference<>();
        
        Px.just(1)
        .doOnCancel(() -> state2.set(state1.get()))
        .subscribe(new Subscriber<Integer>() {
            @Override
            public void onSubscribe(Subscription s) {
                s.cancel();
                state1.set(true);
            }
            
            @Override
            public void onNext(Integer t) {
            }
            
            @Override
            public void onError(Throwable t) {
                e.set(t);
            }
            
            @Override
            public void onComplete() {
                
            }
        });
        
        Assert.assertNull("Error: " + e.get(), e.get());
        
        Assert.assertFalse("Cancel executed before onSubscribe finished", state2.get());
    }

}
