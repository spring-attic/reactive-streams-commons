package rsc.processor;

import java.util.concurrent.ConcurrentLinkedQueue;

import org.junit.Test;

import rsc.test.TestSubscriber;

public class UnicastProcessorTest {

    @Test
    public void secondSubscriberRejectedProperly() {
        
        UnicastProcessor<Integer> up = new UnicastProcessor<>(new ConcurrentLinkedQueue<>());
        
        up.subscribe();
        
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        
        up.subscribe(ts);
        
        ts.assertNoValues()
        .assertError(IllegalStateException.class)
        .assertNotComplete();
        
    }
}