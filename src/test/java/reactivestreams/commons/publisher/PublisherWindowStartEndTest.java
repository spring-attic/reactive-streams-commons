package reactivestreams.commons.publisher;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.*;

import org.junit.*;
import org.reactivestreams.Publisher;

import reactivestreams.commons.processor.SimpleProcessor;
import reactivestreams.commons.subscriber.test.TestSubscriber;
import reactivestreams.commons.support.ConstructorTestBuilder;

public class PublisherWindowStartEndTest {
    @Test
    public void constructors() {
        ConstructorTestBuilder ctb = new ConstructorTestBuilder(PublisherWindowStartEnd.class);
        
        ctb.addRef("source", PublisherNever.instance());
        ctb.addRef("start", PublisherNever.instance());
        ctb.addRef("end", (Function<Object, Publisher<Object>>)o -> PublisherNever.instance());
        ctb.addRef("drainQueueSupplier", (Supplier<Queue<Object>>)() -> new ConcurrentLinkedQueue<>());
        ctb.addRef("processorQueueSupplier", (Supplier<Queue<Object>>)() -> new ConcurrentLinkedQueue<>());
        
        ctb.test();
    }

    static <T> TestSubscriber<T> toList(Publisher<T> windows) {
        TestSubscriber<T> ts = new TestSubscriber<>();
        windows.subscribe(ts);
        return ts;
    }

    @SafeVarargs
    static <T> void expect(TestSubscriber<PublisherBase<T>> ts, int index, T... values) {
        toList(ts.values().get(index))
        .assertValues(values)
        .assertComplete()
        .assertNoError();
    }

    @Test
    public void normal() {
        TestSubscriber<PublisherBase<Integer>> ts = new TestSubscriber<>();
        
        SimpleProcessor<Integer> sp1 = new SimpleProcessor<>();
        SimpleProcessor<Integer> sp2 = new SimpleProcessor<>();
        SimpleProcessor<Integer> sp3 = new SimpleProcessor<>();
        SimpleProcessor<Integer> sp4 = new SimpleProcessor<>();
        
        sp1.window(sp2, v -> v == 1 ? sp3 : sp4).subscribe(ts);
        
        sp1.onNext(1);

        sp2.onNext(1);
        
        sp1.onNext(2);
        
        sp2.onNext(2);
        
        sp1.onNext(3);
        
        sp3.onNext(1);
        
        sp1.onNext(4);
        
        sp4.onNext(1);
        
        sp1.onComplete();
        
        ts.assertValueCount(2)
        .assertNoError()
        .assertComplete();
        
        expect(ts, 0, 2, 3);
        expect(ts, 1, 3, 4);
        
        Assert.assertFalse("sp1 has subscribers?", sp1.hasSubscribers());
        Assert.assertFalse("sp2 has subscribers?", sp2.hasSubscribers());
        Assert.assertFalse("sp3 has subscribers?", sp3.hasSubscribers());
        Assert.assertFalse("sp4 has subscribers?", sp4.hasSubscribers());
    }
    
    @Test
    public void normalStarterEnds() {
        TestSubscriber<PublisherBase<Integer>> ts = new TestSubscriber<>();
        
        SimpleProcessor<Integer> sp1 = new SimpleProcessor<>();
        SimpleProcessor<Integer> sp2 = new SimpleProcessor<>();
        SimpleProcessor<Integer> sp3 = new SimpleProcessor<>();
        SimpleProcessor<Integer> sp4 = new SimpleProcessor<>();
        
        sp1.window(sp2, v -> v == 1 ? sp3 : sp4).subscribe(ts);
        
        sp1.onNext(1);

        sp2.onNext(1);
        
        sp1.onNext(2);
        
        sp2.onNext(2);
        
        sp1.onNext(3);
        
        sp3.onNext(1);
        
        sp1.onNext(4);
        
        sp4.onNext(1);
        
        sp2.onComplete();
        
        ts.assertValueCount(2)
        .assertNoError()
        .assertComplete();
        
        expect(ts, 0, 2, 3);
        expect(ts, 1, 3, 4);

        Assert.assertFalse("sp1 has subscribers?", sp1.hasSubscribers());
        Assert.assertFalse("sp2 has subscribers?", sp2.hasSubscribers());
        Assert.assertFalse("sp3 has subscribers?", sp3.hasSubscribers());
        Assert.assertFalse("sp4 has subscribers?", sp4.hasSubscribers());
    }

    @Test
    public void oneWindowOnly() {
        TestSubscriber<PublisherBase<Integer>> ts = new TestSubscriber<>();
        
        SimpleProcessor<Integer> sp1 = new SimpleProcessor<>();
        SimpleProcessor<Integer> sp2 = new SimpleProcessor<>();
        SimpleProcessor<Integer> sp3 = new SimpleProcessor<>();
        SimpleProcessor<Integer> sp4 = new SimpleProcessor<>();
        
        sp1.window(sp2, v -> v == 1 ? sp3 : sp4).subscribe(ts);
        

        sp2.onNext(1);
        sp2.onComplete();
        
        sp1.onNext(1);
        sp1.onNext(2);
        sp1.onNext(3);
        
        sp3.onComplete();
        
        sp1.onNext(4);
        
        ts.assertValueCount(1)
        .assertNoError()
        .assertComplete();
        
        expect(ts, 0, 1, 2, 3);

        Assert.assertFalse("sp1 has subscribers?", sp1.hasSubscribers());
        Assert.assertFalse("sp2 has subscribers?", sp2.hasSubscribers());
        Assert.assertFalse("sp3 has subscribers?", sp3.hasSubscribers());
        Assert.assertFalse("sp4 has subscribers?", sp4.hasSubscribers());
    }

}
