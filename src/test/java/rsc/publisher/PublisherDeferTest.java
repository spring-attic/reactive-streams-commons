package rsc.publisher;

import org.junit.Test;
import rsc.test.TestSubscriber;

public class PublisherDeferTest {

    @Test(expected = NullPointerException.class)
    public void supplierNull() {
        new PublisherDefer<Integer>(null);
    }

    @Test
    public void supplierReturnsNull() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();

        new PublisherDefer<Integer>(() -> null).subscribe(ts);

        ts
          .assertNoValues()
          .assertNotComplete()
          .assertError(NullPointerException.class);
    }

    @Test
    public void supplierThrows() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();

        new PublisherDefer<Integer>(() -> {
            throw new RuntimeException("forced failure");
        }).subscribe(ts);

        ts
          .assertNoValues()
          .assertNotComplete()
          .assertError(RuntimeException.class)
          .assertErrorMessage("forced failure");
    }

    @Test
    public void normal() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();

        new PublisherDefer<>(() -> new PublisherJust<>(1)).subscribe(ts);

        ts.assertValue(1)
          .assertNoError()
          .assertComplete();
    }
}
