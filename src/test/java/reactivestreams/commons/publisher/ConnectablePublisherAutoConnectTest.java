package reactivestreams.commons.publisher;

import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import org.junit.*;

import reactivestreams.commons.flow.Cancellation;
import reactivestreams.commons.processor.SimpleProcessor;
import reactivestreams.commons.test.TestSubscriber;
import reactivestreams.commons.util.ConstructorTestBuilder;

public class ConnectablePublisherAutoConnectTest {

    @Test
    public void constructors() {
        ConstructorTestBuilder ctb = new ConstructorTestBuilder(ConnectablePublisherAutoConnect.class);
        
        ctb.addRef("source", PublisherNever.instance().publish());
        ctb.addInt("n", 1, Integer.MAX_VALUE);
        ctb.addRef("cancelSupport", (Consumer<Runnable>)r -> { });
        
        ctb.test();
    }
    
    @Test
    public void connectImmediately() {
        SimpleProcessor<Integer> sp = new SimpleProcessor<>();
        
        AtomicReference<Cancellation> cancel = new AtomicReference<>();
        
        sp.publish().autoConnect(0, cancel::set);
        
        Assert.assertNotNull(cancel.get());
        Assert.assertTrue("sp has no subscribers?", sp.hasSubscribers());

        cancel.get().dispose();
        Assert.assertFalse("sp has subscribers?", sp.hasSubscribers());
    }

    @Test
    public void connectAfterMany() {
        SimpleProcessor<Integer> sp = new SimpleProcessor<>();
        
        AtomicReference<Cancellation> cancel = new AtomicReference<>();
        
        Px<Integer> p = sp.publish().autoConnect(2, cancel::set);
        
        Assert.assertNull(cancel.get());
        Assert.assertFalse("sp has subscribers?", sp.hasSubscribers());
        
        p.subscribe(new TestSubscriber<>());
        
        Assert.assertNull(cancel.get());
        Assert.assertFalse("sp has subscribers?", sp.hasSubscribers());

        p.subscribe(new TestSubscriber<>());

        Assert.assertNotNull(cancel.get());
        Assert.assertTrue("sp has no subscribers?", sp.hasSubscribers());
        
        cancel.get().dispose();
        Assert.assertFalse("sp has subscribers?", sp.hasSubscribers());
    }
}
