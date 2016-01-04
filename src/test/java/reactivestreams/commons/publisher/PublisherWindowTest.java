package reactivestreams.commons.publisher;

import java.util.concurrent.ConcurrentLinkedQueue;

import org.junit.Test;

import reactivestreams.commons.subscriber.test.TestSubscriber;

public class PublisherWindowTest {

    @Test(expected = NullPointerException.class)
    public void source1Null() {
        new PublisherWindow<>(null, 1, ConcurrentLinkedQueue::new);
    }

    @Test(expected = NullPointerException.class)
    public void source2Null() {
        new PublisherWindow<>(null, 1, 2, ConcurrentLinkedQueue::new, ConcurrentLinkedQueue::new);
    }

    @Test(expected = NullPointerException.class)
    public void processorQueue1Null() {
        new PublisherWindow<>(PublisherNever.instance(), 1, null);
    }

    @Test(expected = NullPointerException.class)
    public void processorQueue2Null() {
        new PublisherWindow<>(PublisherNever.instance(), 1, 1, null, ConcurrentLinkedQueue::new);
    }

    @Test(expected = NullPointerException.class)
    public void overflowQueueNull() {
        new PublisherWindow<>(PublisherNever.instance(), 1, 1, ConcurrentLinkedQueue::new, null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void size1Invalid() {
        new PublisherWindow<>(PublisherNever.instance(), 0, ConcurrentLinkedQueue::new);
    }

    @Test(expected = IllegalArgumentException.class)
    public void size2Invalid() {
        new PublisherWindow<>(PublisherNever.instance(), 0, 2, ConcurrentLinkedQueue::new, ConcurrentLinkedQueue::new);
    }

    @Test(expected = IllegalArgumentException.class)
    public void skipInvalid() {
        new PublisherWindow<>(PublisherNever.instance(), 1, 0, ConcurrentLinkedQueue::new, ConcurrentLinkedQueue::new);
    }

    @Test
    public void processorQueue1ReturnsNull() {
        TestSubscriber<Object> ts = new TestSubscriber<>();
        
        new PublisherWindow<>(new PublisherRange(1, 10), 1, () -> null).subscribe(ts);
        
        ts.assertNoValues()
        .assertNotComplete()
        .assertError(NullPointerException.class);
    }

    @Test
    public void processorQueue2ReturnsNull() {
        TestSubscriber<Object> ts = new TestSubscriber<>();

        new PublisherWindow<>(new PublisherRange(1, 10), 1, 2, () -> null, ConcurrentLinkedQueue::new).subscribe(ts);

        ts.assertNoValues()
        .assertNotComplete()
        .assertError(NullPointerException.class);
    }

    @Test
    public void overflowQueueReturnsNull() {
        TestSubscriber<Object> ts = new TestSubscriber<>();

        new PublisherWindow<>(new PublisherRange(1, 10), 2, 1, ConcurrentLinkedQueue::new, () -> null).subscribe(ts);

        ts.assertNoValues()
        .assertNotComplete()
        .assertError(NullPointerException.class);
    }

}
