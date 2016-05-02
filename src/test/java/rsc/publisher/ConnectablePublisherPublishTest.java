package rsc.publisher;

import java.util.Queue;
import java.util.concurrent.*;
import java.util.function.Supplier;

import org.junit.*;

import rsc.flow.Cancellation;
import rsc.processor.SimpleProcessor;
import rsc.processor.UnicastProcessor;
import rsc.test.TestSubscriber;
import rsc.util.*;
import rsc.util.SpscArrayQueue;

public class ConnectablePublisherPublishTest {

    @Test
    public void constructors() {
        ConstructorTestBuilder ctb = new ConstructorTestBuilder(ConnectablePublisherPublish.class);
        
        ctb.addRef("source", Px.never());
        ctb.addInt("prefetch", 1, Integer.MAX_VALUE);
        ctb.addRef("queueSupplier", (Supplier<Queue<Object>>)() -> new ConcurrentLinkedQueue<>());
        
        ctb.test();
    }
    
    @Test
    public void normal() {
        TestSubscriber<Integer> ts1 = new TestSubscriber<>();
        TestSubscriber<Integer> ts2 = new TestSubscriber<>();
        
        ConnectablePublisher<Integer> p = Px.range(1, 5).publish();
        
        p.subscribe(ts1);
        p.subscribe(ts2);
        
        ts1
        .assertNoValues()
        .assertNoError()
        .assertNotComplete();

        ts2
        .assertNoValues()
        .assertNoError()
        .assertNotComplete();
        
        p.connect();
        
        ts1.assertValues(1, 2, 3, 4, 5)
        .assertNoError()
        .assertComplete();

        ts2.assertValues(1, 2, 3, 4, 5)
        .assertNoError()
        .assertComplete();
    }
    
    @Test
    public void normalBackpressured() {
        TestSubscriber<Integer> ts1 = new TestSubscriber<>(0);
        TestSubscriber<Integer> ts2 = new TestSubscriber<>(0);
        
        ConnectablePublisher<Integer> p = Px.range(1, 5).publish();
        
        p.subscribe(ts1);
        p.subscribe(ts2);
        
        ts1
        .assertNoValues()
        .assertNoError()
        .assertNotComplete();

        ts2
        .assertNoValues()
        .assertNoError()
        .assertNotComplete();
        
        p.connect();

        ts1
        .assertNoValues()
        .assertNoError()
        .assertNotComplete();

        ts2
        .assertNoValues()
        .assertNoError()
        .assertNotComplete();

        ts1.request(3);
        ts2.request(2);
        
        ts1.assertValues(1, 2)
        .assertNoError()
        .assertNotComplete();

        ts2.assertValues(1, 2)
        .assertNoError()
        .assertNotComplete();
        
        ts1.request(2);
        ts2.request(3);

        ts1.assertValues(1, 2, 3, 4, 5)
        .assertNoError()
        .assertComplete();

        ts2.assertValues(1, 2, 3, 4, 5)
        .assertNoError()
        .assertComplete();
    }

    @Test
    public void normalAsyncFused() {
        TestSubscriber<Integer> ts1 = new TestSubscriber<>();
        TestSubscriber<Integer> ts2 = new TestSubscriber<>();
        
        UnicastProcessor<Integer> up = new UnicastProcessor<>(new SpscArrayQueue<>(8));
        up.onNext(1);
        up.onNext(2);
        up.onNext(3);
        up.onNext(4);
        up.onNext(5);
        up.onComplete();
        
        ConnectablePublisher<Integer> p = up.publish();
        
        p.subscribe(ts1);
        p.subscribe(ts2);
        
        ts1
        .assertNoValues()
        .assertNoError()
        .assertNotComplete();

        ts2
        .assertNoValues()
        .assertNoError()
        .assertNotComplete();
        
        p.connect();
        
        ts1.assertValues(1, 2, 3, 4, 5)
        .assertNoError()
        .assertComplete();

        ts2.assertValues(1, 2, 3, 4, 5)
        .assertNoError()
        .assertComplete();
    }
    
    @Test
    public void normalBackpressuredAsyncFused() {
        TestSubscriber<Integer> ts1 = new TestSubscriber<>(0);
        TestSubscriber<Integer> ts2 = new TestSubscriber<>(0);

        UnicastProcessor<Integer> up = new UnicastProcessor<>(new SpscArrayQueue<>(8));
        up.onNext(1);
        up.onNext(2);
        up.onNext(3);
        up.onNext(4);
        up.onNext(5);
        up.onComplete();

        ConnectablePublisher<Integer> p = up.publish();
        
        p.subscribe(ts1);
        p.subscribe(ts2);
        
        ts1
        .assertNoValues()
        .assertNoError()
        .assertNotComplete();

        ts2
        .assertNoValues()
        .assertNoError()
        .assertNotComplete();
        
        p.connect();

        ts1
        .assertNoValues()
        .assertNoError()
        .assertNotComplete();

        ts2
        .assertNoValues()
        .assertNoError()
        .assertNotComplete();

        ts1.request(3);
        ts2.request(2);
        
        ts1.assertValues(1, 2)
        .assertNoError()
        .assertNotComplete();

        ts2.assertValues(1, 2)
        .assertNoError()
        .assertNotComplete();
        
        ts1.request(2);
        ts2.request(3);

        ts1.assertValues(1, 2, 3, 4, 5)
        .assertNoError()
        .assertComplete();

        ts2.assertValues(1, 2, 3, 4, 5)
        .assertNoError()
        .assertComplete();
    }

    @Test
    public void normalHidden() {
        TestSubscriber<Integer> ts1 = new TestSubscriber<>();
        TestSubscriber<Integer> ts2 = new TestSubscriber<>();
        
        ConnectablePublisher<Integer> p = Px.range(1, 5).hide().publish();
        
        p.subscribe(ts1);
        p.subscribe(ts2);
        
        ts1
        .assertNoValues()
        .assertNoError()
        .assertNotComplete();

        ts2
        .assertNoValues()
        .assertNoError()
        .assertNotComplete();
        
        p.connect();
        
        ts1.assertValues(1, 2, 3, 4, 5)
        .assertNoError()
        .assertComplete();

        ts2.assertValues(1, 2, 3, 4, 5)
        .assertNoError()
        .assertComplete();
    }
    
    @Test
    public void normalHiddenBackpressured() {
        TestSubscriber<Integer> ts1 = new TestSubscriber<>(0);
        TestSubscriber<Integer> ts2 = new TestSubscriber<>(0);
        
        ConnectablePublisher<Integer> p = Px.range(1, 5).hide().publish();
        
        p.subscribe(ts1);
        p.subscribe(ts2);
        
        ts1
        .assertNoValues()
        .assertNoError()
        .assertNotComplete();

        ts2
        .assertNoValues()
        .assertNoError()
        .assertNotComplete();
        
        p.connect();

        ts1
        .assertNoValues()
        .assertNoError()
        .assertNotComplete();

        ts2
        .assertNoValues()
        .assertNoError()
        .assertNotComplete();

        ts1.request(3);
        ts2.request(2);
        
        ts1.assertValues(1, 2)
        .assertNoError()
        .assertNotComplete();

        ts2.assertValues(1, 2)
        .assertNoError()
        .assertNotComplete();
        
        ts1.request(2);
        ts2.request(3);

        ts1.assertValues(1, 2, 3, 4, 5)
        .assertNoError()
        .assertComplete();

        ts2.assertValues(1, 2, 3, 4, 5)
        .assertNoError()
        .assertComplete();
    }

    @Test
    public void disconnect() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        
        SimpleProcessor<Integer> sp = new SimpleProcessor<>();
        
        ConnectablePublisher<Integer> p = sp.publish();
        
        p.subscribe(ts);
        
        Cancellation r = p.connect();
                
        sp.onNext(1);
        sp.onNext(2);
        
        r.dispose();
        
        ts.assertValues(1, 2)
        .assertError(CancellationException.class)
        .assertNotComplete();
        
        Assert.assertFalse("sp has subscribers?", sp.hasDownstreams());
    }

    @Test
    public void disconnectBackpressured() {
        TestSubscriber<Integer> ts = new TestSubscriber<>(0);
        
        SimpleProcessor<Integer> sp = new SimpleProcessor<>();
        
        ConnectablePublisher<Integer> p = sp.publish();
        
        p.subscribe(ts);
        
        Cancellation r = p.connect();
                
        r.dispose();
        
        ts.assertNoValues()
        .assertError(CancellationException.class)
        .assertNotComplete();

        Assert.assertFalse("sp has subscribers?", sp.hasDownstreams());
    }

    @Test
    public void error() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        
        SimpleProcessor<Integer> sp = new SimpleProcessor<>();
        
        ConnectablePublisher<Integer> p = sp.publish();
        
        p.subscribe(ts);
        
        p.connect();
                
        sp.onNext(1);
        sp.onNext(2);
        sp.onError(new RuntimeException("forced failure"));
        
        ts.assertValues(1, 2)
        .assertError(RuntimeException.class)
        .assertErrorMessage("forced failure")
        .assertNotComplete();
    }

    @Test
    public void fusedMapInvalid() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        
        ConnectablePublisher<Integer> p = Px.range(1, 5).map(v -> (Integer)null).publish();
        
        p.subscribe(ts);
        
        p.connect();
                
        ts.assertNoValues()
        .assertError(NullPointerException.class)
        .assertNotComplete();
    }

}
