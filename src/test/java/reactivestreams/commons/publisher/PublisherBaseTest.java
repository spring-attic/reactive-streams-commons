package reactivestreams.commons.publisher;

import org.junit.Test;

import reactivestreams.commons.test.TestSubscriber;
import reactivestreams.commons.util.EmptySubscription;

public class PublisherBaseTest {

    @Test
    public void subscribeLambdaJust() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        ts.onSubscribe(EmptySubscription.INSTANCE);
        
        Px.just(1).subscribe(ts::onNext, ts::onError, ts::onComplete);
        
        ts.assertValue(1)
        .assertNoError()
        .assertComplete();
    }

    @Test
    public void subscribeLambdaEmpty() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        ts.onSubscribe(EmptySubscription.INSTANCE);
        
        Px.<Integer>empty().subscribe(ts::onNext, ts::onError, ts::onComplete);
        
        ts.assertNoValues()
        .assertNoError()
        .assertComplete();
    }

    @Test
    public void subscribeLambdaError() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        ts.onSubscribe(EmptySubscription.INSTANCE);
        
        Px.<Integer>error(new RuntimeException("forced failure"))
        .subscribe(ts::onNext, ts::onError, ts::onComplete);
        
        ts.assertNoValues()
        .assertError(RuntimeException.class)
        .assertErrorMessage("forced failure")
        .assertNotComplete();
    }

}
