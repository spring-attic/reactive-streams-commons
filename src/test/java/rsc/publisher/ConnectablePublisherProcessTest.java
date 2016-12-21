package rsc.publisher;

import java.util.concurrent.CancellationException;
import java.util.function.*;

import org.junit.*;
import org.reactivestreams.Processor;

import rsc.flow.Disposable;
import rsc.processor.DirectProcessor;
import rsc.processor.UnicastProcessor;
import rsc.test.TestSubscriber;
import rsc.util.*;
import rsc.util.SpscArrayQueue;

public class ConnectablePublisherProcessTest {

    @Test
    public void constructors() {
        ConstructorTestBuilder ctb = new ConstructorTestBuilder(ConnectablePublisherProcess.class);
        
        ctb.addRef("source", Px.never());
        ctb.addRef("processorSupplier", (Supplier<Processor<Object,Object>>)() -> new DirectProcessor<>());
        ctb.addRef("selector", Function.identity());
        
        ctb.test();
    }
    
    @Test
    public void normal() {
        TestSubscriber<Integer> ts1 = new TestSubscriber<>();
        TestSubscriber<Integer> ts2 = new TestSubscriber<>();
        
        ConnectablePublisher<Integer> p = Px.range(1, 5).process(new DirectProcessor<>());
        
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

        ConnectablePublisher<Integer> p = Px.range(1, 5).process(new UnicastProcessor<Integer>(new SpscArrayQueue<>(8)));

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

        ConnectablePublisher<Integer> p = Px.range(1, 5).process(new DirectProcessor<>());

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
        
        ConnectablePublisher<Integer> p = up.process(new UnicastProcessor<>(new SpscArrayQueue<>(8)));
        
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

        ConnectablePublisher<Integer> p = up.process(new UnicastProcessor<>(new SpscArrayQueue<>(8)));
        
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
        
        DirectProcessor<Integer> sp = new DirectProcessor<>();
        
        ConnectablePublisher<Integer> p = sp.process(new DirectProcessor<Integer>());
        
        p.subscribe(ts);
        
        Disposable r = p.connect();
                
        sp.onNext(1);
        sp.onNext(2);
        
        r.dispose();
        
        ts.assertValues(1, 2)
        .assertError(CancellationException.class)
        .assertNotComplete();
        
        Assert.assertFalse("sp has subscribers?", sp.hasDownstreams());
    }


    @Test
    public void cancel() {
        TestSubscriber<Integer> ts1 = new TestSubscriber<>();
        TestSubscriber<Integer> ts2 = new TestSubscriber<>();

        DirectProcessor<Integer> sp = new DirectProcessor<>();

        ConnectablePublisher<Integer> p = sp.process(new DirectProcessor<Integer>());

        p.subscribe(ts1);
        p.subscribe(ts2);

        Disposable r = p.connect();

        sp.onNext(1);
        sp.onNext(2);

        ts1.cancel();

        Assert.assertTrue("sp has no subscribers?", sp.hasDownstreams());

        r.dispose();

        ts1.assertValues(1, 2)
          .assertNoError()
          .assertNotComplete();

        ts2.assertValues(1, 2)
           .assertError(CancellationException.class)
          .assertNotComplete();

        Assert.assertFalse("sp has subscribers?", sp.hasDownstreams());
    }


    @Test
    public void disconnectBackpressured() {
        TestSubscriber<Integer> ts = new TestSubscriber<>(0);
        
        DirectProcessor<Integer> sp = new DirectProcessor<>();

        ConnectablePublisher<Integer> p = sp.process(new DirectProcessor<Integer>());
        
        p.subscribe(ts);
        
        Disposable r = p.connect();
                
        r.dispose();
        
        ts.assertNoValues()
        .assertError(CancellationException.class)
        .assertNotComplete();

        Assert.assertFalse("sp has subscribers?", sp.hasDownstreams());
    }

    @Test
    public void error() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        
        DirectProcessor<Integer> sp = new DirectProcessor<>();

        ConnectablePublisher<Integer> p = sp.process(new DirectProcessor<Integer>());
        
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
        
        ConnectablePublisher<Integer> p = Px.range(1, 5).map(v -> (Integer)null).process(new DirectProcessor<Integer>());
        
        p.subscribe(ts);
        
        p.connect();
                
        ts.assertNoValues()
        .assertError(NullPointerException.class)
        .assertNotComplete();
    }

}
