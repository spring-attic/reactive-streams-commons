package reactivestreams.commons.publisher;

import java.util.concurrent.CancellationException;
import java.util.function.*;

import org.junit.*;
import org.reactivestreams.Processor;

import reactivestreams.commons.flow.Cancellation;
import reactivestreams.commons.processor.*;
import reactivestreams.commons.test.TestSubscriber;
import reactivestreams.commons.util.*;

public class ConnectablePublisherMulticastTest {

    @Test
    public void constructors() {
        ConstructorTestBuilder ctb = new ConstructorTestBuilder(ConnectablePublisherMulticast.class);
        
        ctb.addRef("source", Px.never());
        ctb.addRef("processorSupplier", (Supplier<Processor<Object,Object>>)() -> new SimpleProcessor<>());
        ctb.addRef("selector", Function.identity());
        
        ctb.test();
    }
    
    @Test
    public void normal() {
        TestSubscriber<Integer> ts1 = new TestSubscriber<>();
        TestSubscriber<Integer> ts2 = new TestSubscriber<>();
        
        ConnectablePublisher<Integer> p = Px.range(1, 5).multicast(new SimpleProcessor<>());
        
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
    public void normalProcessorBackpressured() {
        TestSubscriber<Integer> ts1 = new TestSubscriber<>(0);

        ConnectablePublisher<Integer> p = Px.range(1, 5).multicast(new UnicastProcessor<Integer>(new SpscArrayQueue<>(8)));

        p.subscribe(ts1);

        ts1
        .assertNoValues()
        .assertNoError()
        .assertNotComplete();

        p.connect();

        ts1
        .assertNoValues()
        .assertNoError()
        .assertNotComplete();

        ts1.request(3);

        ts1.assertValues(1, 2, 3)
        .assertNoError()
        .assertNotComplete();

        ts1.request(2);

        ts1.assertValues(1, 2, 3, 4, 5)
        .assertNoError()
        .assertComplete();

    }

    @Test
    public void errorBackpressured() {
        TestSubscriber<Integer> ts1 = new TestSubscriber<>(0);
        TestSubscriber<Integer> ts2 = new TestSubscriber<>(0);

        ConnectablePublisher<Integer> p = Px.range(1, 5).multicast(new SimpleProcessor<>());

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
        .assertError(IllegalStateException.class)
        .assertNotComplete();

        ts2
        .assertNoValues()
        .assertError(IllegalStateException.class)
        .assertNotComplete();
    }

    @Test
    public void normalAsyncFused() {
        TestSubscriber<Integer> ts1 = new TestSubscriber<>();

        UnicastProcessor<Integer> up = new UnicastProcessor<>(new SpscArrayQueue<>(8));
        up.onNext(1);
        up.onNext(2);
        up.onNext(3);
        up.onNext(4);
        up.onNext(5);
        up.onComplete();
        
        ConnectablePublisher<Integer> p = up.multicast(new UnicastProcessor<>(new SpscArrayQueue<>(8)));
        
        p.subscribe(ts1);

        ts1
        .assertNoValues()
        .assertNoError()
        .assertNotComplete();


        p.connect();
        
        ts1.assertValues(1, 2, 3, 4, 5)
        .assertNoError()
        .assertComplete();

    }
    
    @Test
    public void normalBackpressuredAsyncFused() {
        TestSubscriber<Integer> ts1 = new TestSubscriber<>(0);

        UnicastProcessor<Integer> up = new UnicastProcessor<>(new SpscArrayQueue<>(8));
        up.onNext(1);
        up.onNext(2);
        up.onNext(3);
        up.onNext(4);
        up.onNext(5);
        up.onComplete();

        ConnectablePublisher<Integer> p = up.multicast(new UnicastProcessor<>(new SpscArrayQueue<>(8)));
        
        p.subscribe(ts1);

        ts1
        .assertNoValues()
        .assertNoError()
        .assertNotComplete();


        p.connect();

        ts1
        .assertNoValues()
        .assertNoError()
        .assertNotComplete();

        ts1.request(3);

        ts1.assertValues(1, 2, 3)
        .assertNoError()
        .assertNotComplete();

        ts1.request(2);

        ts1.assertValues(1, 2, 3, 4, 5)
        .assertNoError()
        .assertComplete();
    }

    @Test
    public void disconnect() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        
        SimpleProcessor<Integer> sp = new SimpleProcessor<>();
        
        ConnectablePublisher<Integer> p = sp.multicast(new SimpleProcessor<Integer>());
        
        p.subscribe(ts);
        
        Cancellation r = p.connect();
                
        sp.onNext(1);
        sp.onNext(2);
        
        r.dispose();
        
        ts.assertValues(1, 2)
        .assertError(CancellationException.class)
        .assertNotComplete();
        
        Assert.assertFalse("sp has subscribers?", sp.hasSubscribers());
    }


    @Test
    public void cancel() {
        TestSubscriber<Integer> ts1 = new TestSubscriber<>();
        TestSubscriber<Integer> ts2 = new TestSubscriber<>();

        SimpleProcessor<Integer> sp = new SimpleProcessor<>();

        ConnectablePublisher<Integer> p = sp.multicast(new SimpleProcessor<Integer>());

        p.subscribe(ts1);
        p.subscribe(ts2);

        Cancellation r = p.connect();

        sp.onNext(1);
        sp.onNext(2);

        ts1.cancel();

        Assert.assertTrue("sp has no subscribers?", sp.hasSubscribers());

        r.dispose();

        ts1.assertValues(1, 2)
          .assertNoError()
          .assertNotComplete();

        ts2.assertValues(1, 2)
           .assertError(CancellationException.class)
          .assertNotComplete();

        Assert.assertFalse("sp has subscribers?", sp.hasSubscribers());
    }


    @Test
    public void disconnectBackpressured() {
        TestSubscriber<Integer> ts = new TestSubscriber<>(0);
        
        SimpleProcessor<Integer> sp = new SimpleProcessor<>();

        ConnectablePublisher<Integer> p = sp.multicast(new SimpleProcessor<Integer>());
        
        p.subscribe(ts);
        
        Cancellation r = p.connect();
                
        r.dispose();
        
        ts.assertNoValues()
        .assertError(CancellationException.class)
        .assertNotComplete();

        Assert.assertFalse("sp has subscribers?", sp.hasSubscribers());
    }

    @Test
    public void error() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        
        SimpleProcessor<Integer> sp = new SimpleProcessor<>();

        ConnectablePublisher<Integer> p = sp.multicast(new SimpleProcessor<Integer>());
        
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
        
        ConnectablePublisher<Integer> p = Px.range(1, 5).map(v -> (Integer)null).multicast(new SimpleProcessor<Integer>());
        
        p.subscribe(ts);
        
        p.connect();
                
        ts.assertNoValues()
        .assertError(NullPointerException.class)
        .assertNotComplete();
    }

}
