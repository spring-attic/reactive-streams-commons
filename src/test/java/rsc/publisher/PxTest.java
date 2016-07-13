package rsc.publisher;

import org.junit.Test;

import rsc.subscriber.SubscriptionHelper;
import rsc.test.TestSubscriber;


public class PxTest {

    @Test
    public void subscribeLambdaJust() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        ts.onSubscribe(SubscriptionHelper.empty());
        
        Px.just(1).subscribe(ts::onNext, ts::onError, ts::onComplete);
        
        ts.assertValue(1)
        .assertNoError()
        .assertComplete();
    }

    @Test
    public void subscribeLambdaEmpty() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        ts.onSubscribe(SubscriptionHelper.empty());
        
        Px.<Integer>empty().subscribe(ts::onNext, ts::onError, ts::onComplete);
        
        ts.assertNoValues()
        .assertNoError()
        .assertComplete();
    }

    @Test
    public void subscribeLambdaError() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        ts.onSubscribe(SubscriptionHelper.empty());
        
        Px.<Integer>error(new RuntimeException("forced failure"))
        .subscribe(ts::onNext, ts::onError, ts::onComplete);
        
        ts.assertNoValues()
        .assertError(RuntimeException.class)
        .assertErrorMessage("forced failure")
        .assertNotComplete();
    }

}
