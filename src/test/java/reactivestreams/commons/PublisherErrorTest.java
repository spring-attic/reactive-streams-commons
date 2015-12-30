package reactivestreams.commons;

import org.junit.Test;
import reactivestreams.commons.internal.subscriber.test.TestSubscriber;

import java.util.function.Supplier;

public class PublisherErrorTest {

    @Test(expected = NullPointerException.class)
    public void errorNull() {
        new PublisherError<Integer>((Throwable) null);
    }

    @Test(expected = NullPointerException.class)
    public void supplierNull() {
        new PublisherError<Integer>((Supplier<Throwable>) null);
    }

    @Test
    public void normal() {
        TestSubscriber<Object> ts = new TestSubscriber<>();

        RuntimeException ex = new RuntimeException();

        new PublisherError<>(ex).subscribe(ts);

        ts.assertNoValues()
          .assertError(ex)
          .assertNotComplete();
    }

    @Test
    public void normalSupplier() {
        TestSubscriber<Object> ts = new TestSubscriber<>();

        RuntimeException ex = new RuntimeException();

        new PublisherError<>(() -> ex).subscribe(ts);

        ts.assertNoValues()
          .assertError(ex)
          .assertNotComplete();
    }

    @Test
    public void supplierReturnsNull() {
        TestSubscriber<Object> ts = new TestSubscriber<>();

        new PublisherError<>(() -> null).subscribe(ts);

        ts.assertNoValues()
          .assertError(NullPointerException.class)
          .assertNotComplete();
    }

    @Test
    public void supplierThrows() {
        TestSubscriber<Object> ts = new TestSubscriber<>();

        new PublisherError<>(() -> {
            throw new RuntimeException("forced failure");
        }).subscribe(ts);

        ts.assertNoValues()
          .assertError(RuntimeException.class)
          .assertErrorMessage("forced failure")
          .assertNotComplete();
    }

}
