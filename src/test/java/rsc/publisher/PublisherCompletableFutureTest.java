package rsc.publisher;

import java.util.concurrent.CompletableFuture;

import org.junit.Test;
import rsc.test.TestSubscriber;

public class PublisherCompletableFutureTest {

    @Test(expected = NullPointerException.class)
    public void nullFuture() {
        new PublisherCompletableFuture<Integer>(null);
    }

    @Test
    public void normal() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        CompletableFuture<Integer> f = new CompletableFuture<>();

        new PublisherCompletableFuture<>(f).subscribe(ts);

        ts.assertNoValues()
          .assertNoError()
          .assertNotComplete();

        f.complete(1);

        ts.assertValue(1)
          .assertNoError()
          .assertComplete();
    }

    @Test
    public void normalBackpressured() {
        TestSubscriber<Integer> ts = new TestSubscriber<>(0);
        CompletableFuture<Integer> f = new CompletableFuture<>();

        new PublisherCompletableFuture<>(f).subscribe(ts);

        ts.assertNoValues()
          .assertNoError()
          .assertNotComplete();

        f.complete(1);

        ts.assertNoValues()
          .assertNoError()
          .assertNotComplete();

        ts.request(1);

        ts.assertValue(1)
          .assertNoError()
          .assertComplete();
    }

    @Test
    public void futureProducesNull() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        CompletableFuture<Integer> f = new CompletableFuture<>();

        new PublisherCompletableFuture<>(f).subscribe(ts);

        ts.assertNoValues()
          .assertNoError()
          .assertNotComplete();

        f.complete(null);

        ts.assertNoValues()
          .assertError(NullPointerException.class)
          .assertNotComplete();
    }

    @Test
    public void futureProducesException() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        CompletableFuture<Integer> f = new CompletableFuture<>();

        new PublisherCompletableFuture<>(f).subscribe(ts);

        ts.assertNoValues()
          .assertNoError()
          .assertNotComplete();

        f.completeExceptionally(new RuntimeException("forced failure"));

        ts.assertNoValues()
          .assertError(RuntimeException.class)
          .assertErrorMessage("forced failure")
          .assertNotComplete();
    }

}
