package rsc.publisher;

import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import org.junit.*;

import rsc.flow.Cancellation;
import rsc.processor.DirectProcessor;
import rsc.test.TestSubscriber;
import rsc.util.ConstructorTestBuilder;

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
        DirectProcessor<Integer> sp = new DirectProcessor<>();
        
        AtomicReference<Cancellation> cancel = new AtomicReference<>();
        
        sp.publish().autoConnect(0, cancel::set);
        
        Assert.assertNotNull(cancel.get());
        Assert.assertTrue("sp has no subscribers?", sp.hasDownstreams());

        cancel.get().dispose();
        Assert.assertFalse("sp has subscribers?", sp.hasDownstreams());
    }

    @Test
    public void connectAfterMany() {
        DirectProcessor<Integer> sp = new DirectProcessor<>();
        
        AtomicReference<Cancellation> cancel = new AtomicReference<>();
        
        Px<Integer> p = sp.publish().autoConnect(2, cancel::set);
        
        Assert.assertNull(cancel.get());
        Assert.assertFalse("sp has subscribers?", sp.hasDownstreams());
        
        p.subscribe(new TestSubscriber<>());
        
        Assert.assertNull(cancel.get());
        Assert.assertFalse("sp has subscribers?", sp.hasDownstreams());

        p.subscribe(new TestSubscriber<>());

        Assert.assertNotNull(cancel.get());
        Assert.assertTrue("sp has no subscribers?", sp.hasDownstreams());
        
        cancel.get().dispose();
        Assert.assertFalse("sp has subscribers?", sp.hasDownstreams());
    }
}
