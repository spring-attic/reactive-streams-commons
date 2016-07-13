package rsc.publisher;

import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import org.junit.Assert;
import org.junit.Test;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import rsc.publisher.PublisherConcatMap.ErrorMode;
import rsc.subscriber.SubscriptionHelper;
import rsc.test.TestSubscriber;
import rsc.util.ConstructorTestBuilder;


public class PublisherGroupByTest {

    @Test
    public void constructors() {
        ConstructorTestBuilder ctb = new  ConstructorTestBuilder(PublisherGroupBy.class);
        
        ctb.addRef("source", Px.never());
        ctb.addRef("keySelector", (Function<Object, Object>)v -> v);
        ctb.addRef("valueSelector", (Function<Object, Object>)v -> v);
        ctb.addRef("mainQueueSupplier", Px.defaultQueueSupplier(1));
        ctb.addRef("groupQueueSupplier", Px.defaultQueueSupplier(1));
        ctb.addInt("prefetch", 1, Integer.MAX_VALUE);
        
        ctb.test();
    }
    
    @Test
    public void normal() {
        TestSubscriber<GroupedPublisher<Integer, Integer>> ts = new TestSubscriber<>();
        
        Px.range(1, 10).groupBy(k -> k % 2).subscribe(ts);
        
        ts.assertValueCount(2)
        .assertNoError()
        .assertComplete();
        
        TestSubscriber<Integer> ts1 = new TestSubscriber<>();
        ts.values().get(0).subscribe(ts1);
        ts1.assertResult(1, 3, 5, 7, 9);

        
        TestSubscriber<Integer> ts2 = new TestSubscriber<>();
        ts.values().get(1).subscribe(ts2);
        ts2.assertResult(2, 4, 6, 8, 10);
    }

    @Test
    public void normalValueSelector() {
        TestSubscriber<GroupedPublisher<Integer, Integer>> ts = new TestSubscriber<>();
        
        Px.range(1, 10).groupBy(k -> k % 2, v -> -v).subscribe(ts);
        
        ts.assertValueCount(2)
        .assertNoError()
        .assertComplete();
        
        TestSubscriber<Integer> ts1 = new TestSubscriber<>();
        ts.values().get(0).subscribe(ts1);
        ts1.assertResult(-1, -3, -5, -7, -9);

        
        TestSubscriber<Integer> ts2 = new TestSubscriber<>();
        ts.values().get(1).subscribe(ts2);
        ts2.assertResult(-2, -4, -6, -8, -10);
    }

    @Test
    public void takeTwoGroupsOnly() {
        TestSubscriber<GroupedPublisher<Integer, Integer>> ts = new TestSubscriber<>();
        
        Px.range(1, 10).groupBy(k -> k % 3).take(2).subscribe(ts);
        
        ts.assertValueCount(2)
        .assertNoError()
        .assertComplete();
        
        TestSubscriber<Integer> ts1 = new TestSubscriber<>();
        ts.values().get(0).subscribe(ts1);
        ts1.assertResult(1, 4, 7, 10);

        
        TestSubscriber<Integer> ts2 = new TestSubscriber<>();
        ts.values().get(1).subscribe(ts2);
        ts2.assertResult(2, 5, 8);
    }

    @Test
    public void keySelectorNull() {
        TestSubscriber<GroupedPublisher<Integer, Integer>> ts = new TestSubscriber<>();
        
        Px.range(1, 10).groupBy(k -> (Integer)null).subscribe(ts);
        
        ts.assertFailure(NullPointerException.class);
    }

    @Test
    public void valueSelectorNull() {
        TestSubscriber<GroupedPublisher<Integer, Integer>> ts = new TestSubscriber<>();
        
        Px.range(1, 10).groupBy(k -> 1, v -> (Integer)null).subscribe(ts);
        
        ts.assertFailure(NullPointerException.class);
    }

    @Test
    public void error() {
        TestSubscriber<GroupedPublisher<Integer, Integer>> ts = new TestSubscriber<>();
        
        Px.<Integer>error(new RuntimeException("forced failure")).groupBy(k -> k).subscribe(ts);
        
        ts.assertFailureMessage(RuntimeException.class, "forced failure");
    }

    @Test
    public void backpressure() {
        TestSubscriber<GroupedPublisher<Integer, Integer>> ts = new TestSubscriber<>(0L);
        
        Px.range(1, 10).groupBy(k -> 1).subscribe(ts);
        
        ts.assertNoEvents();
        
        ts.request(1);
        
        ts.assertValueCount(1)
        .assertNoError()
        .assertComplete();
        
        TestSubscriber<Integer> ts1 = new TestSubscriber<>(0L);
        
        ts.values().get(0).subscribe(ts1);
        
        ts1.assertNoEvents();
        
        ts1.request(10);
        
        ts1.assertResult(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
    }
    
    @Test
    public void flatMapBack() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        
        Px.range(1, 10).groupBy(k -> k % 2).flatMap(g -> g).subscribe(ts);
        
        ts.assertResult(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
    }

    @Test
    public void flatMapBackHidden() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        
        Px.range(1, 10).groupBy(k -> k % 2).flatMap(g -> g.hide()).subscribe(ts);
        
        ts.assertResult(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
    }

    @Test
    public void concatMapBack() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        
        Px.range(1, 10).groupBy(k -> k % 2).concatMap(g -> g).subscribe(ts);
        
        ts.assertResult(1, 3, 5, 7, 9, 2, 4, 6, 8, 10);
    }

    @Test
    public void concatMapBackHidden() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        
        Px.range(1, 10).groupBy(k -> k % 2).hide().concatMap(g -> g).subscribe(ts);
        
        ts.assertResult(1, 3, 5, 7, 9, 2, 4, 6, 8, 10);
    }
    
    @Test
    public void empty() {
        TestSubscriber<GroupedPublisher<Integer, Integer>> ts = new TestSubscriber<>(0L);

        Px.<Integer>empty().groupBy(v -> v).subscribe(ts);
        
        ts.assertResult();
    }

    @Test
    public void oneGroupLongMerge() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        
        Px.range(1, 1_000_000).groupBy(v -> 1).flatMap(g -> g).subscribe(ts);
        
        ts.assertValueCount(1_000_000)
        .assertNoError()
        .assertComplete();
    }

    @Test
    public void oneGroupLongMergeHidden() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        
        Px.range(1, 1_000_000).groupBy(v -> 1).flatMap(g -> g.hide()).subscribe(ts);
        
        ts.assertValueCount(1_000_000)
        .assertNoError()
        .assertComplete();
    }

    
    @Test
    public void twoGroupsLongMerge() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        
        Px.range(1, 1_000_000).groupBy(v -> (v & 1)).flatMap(g -> g).subscribe(ts);
        
        ts.assertValueCount(1_000_000)
        .assertNoError()
        .assertComplete();
    }

    @Test
    public void twoGroupsLongMergeHidden() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        
        Px.range(1, 1_000_000).groupBy(v -> (v & 1)).flatMap(g -> g.hide()).subscribe(ts);
        
        ts.assertValueCount(1_000_000)
        .assertNoError()
        .assertComplete();
    }

    @Test
    public void twoGroupsLongAsyncMergeLoop() {
        for (int i = 0; i < 100; i++) {
            twoGroupsLongAsyncMerge();
        }
    }

    @Test
    public void twoGroupsLongAsyncMerge() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        
        Px.range(1, 1_000_000).groupBy(v -> (v & 1))
        .flatMap(g -> g).observeOn(ForkJoinPool.commonPool()).subscribe(ts);
        
        ts.await(5, TimeUnit.SECONDS);
        
        ts.assertValueCount(1_000_000)
        .assertNoError()
        .assertComplete();
    }

    @Test
    public void twoGroupsLongAsyncMergeHiddenLoop() {
        for (int i = 0; i < 100; i++) {
            twoGroupsLongAsyncMergeHidden();
        }
    }

    @Test
    public void twoGroupsLongAsyncMergeHidden() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        
        Px.range(1, 1_000_000).groupBy(v -> (v & 1))
        .flatMap(g -> g.hide()).observeOn(ForkJoinPool.commonPool()).subscribe(ts);
        
        ts.assertTerminated(5, TimeUnit.SECONDS);
        
        ts.assertValueCount(1_000_000)
        .assertNoError()
        .assertComplete();
    }

    @Test
    public void twoGroupsConsumeWithSubscribe() {
        
        TestSubscriber<Integer> ts1 = new TestSubscriber<>();
        TestSubscriber<Integer> ts2 = new TestSubscriber<>();
        TestSubscriber<Integer> ts3 = new TestSubscriber<>();
        ts3.onSubscribe(SubscriptionHelper.empty());
        
        
        Px.range(0, 1_000_000).groupBy(v -> v & 1).subscribe(new Subscriber<GroupedPublisher<Integer, Integer>>() {
            @Override
            public void onSubscribe(Subscription s) {
                s.request(Long.MAX_VALUE);
            }
            
            @Override
            public void onNext(GroupedPublisher<Integer, Integer> t) {
                if (t.key() == 0) {
                    t.observeOn(ForkJoinPool.commonPool()).subscribe(ts1);
                } else {
                    t.observeOn(ForkJoinPool.commonPool()).subscribe(ts2);
                }
            }
            
            @Override
            public void onError(Throwable t) {
                ts3.onError(t);
            }
            
            @Override
            public void onComplete() {
                ts3.onComplete();
            }
        });
        
        ts1.await(5, TimeUnit.SECONDS);
        ts2.await(5, TimeUnit.SECONDS);
        ts3.await(5, TimeUnit.SECONDS);
        
        ts1.assertValueCount(500_000)
        .assertNoError()
        .assertComplete();

        ts2.assertValueCount(500_000)
        .assertNoError()
        .assertComplete();

        ts3.assertNoValues()
        .assertNoError()
        .assertComplete();

    }

    @Test
    public void twoGroupsConsumeWithSubscribePrefetchSmaller() {
        
        TestSubscriber<Integer> ts1 = new TestSubscriber<>();
        TestSubscriber<Integer> ts2 = new TestSubscriber<>();
        TestSubscriber<Integer> ts3 = new TestSubscriber<>();
        ts3.onSubscribe(SubscriptionHelper.empty());
        
        
        Px.range(0, 1_000_000).groupBy(v -> v & 1).subscribe(new Subscriber<GroupedPublisher<Integer, Integer>>() {
            @Override
            public void onSubscribe(Subscription s) {
                s.request(Long.MAX_VALUE);
            }
            
            @Override
            public void onNext(GroupedPublisher<Integer, Integer> t) {
                if (t.key() == 0) {
                    t.observeOn(ForkJoinPool.commonPool(), false, 32).subscribe(ts1);
                } else {
                    t.observeOn(ForkJoinPool.commonPool(), false, 32).subscribe(ts2);
                }
            }
            
            @Override
            public void onError(Throwable t) {
                ts3.onError(t);
            }
            
            @Override
            public void onComplete() {
                ts3.onComplete();
            }
        });
        
        if (!ts1.await(5, TimeUnit.SECONDS)) {
            Assert.fail("main subscriber timed out");
        }
        if (!ts2.await(5, TimeUnit.SECONDS)) {
            Assert.fail("group 0 subscriber timed out");
        }
        if (!ts3.await(5, TimeUnit.SECONDS)) {
            Assert.fail("group 1 subscriber timed out");
        }
        
        ts1.assertValueCount(500_000)
        .assertNoError()
        .assertComplete();

        ts2.assertValueCount(500_000)
        .assertNoError()
        .assertComplete();

        ts3.assertNoValues()
        .assertNoError()
        .assertComplete();

    }

    @Test
    public void twoGroupsConsumeWithSubscribePrefetchBigger() {
        
        TestSubscriber<Integer> ts1 = new TestSubscriber<>();
        TestSubscriber<Integer> ts2 = new TestSubscriber<>();
        TestSubscriber<Integer> ts3 = new TestSubscriber<>();
        ts3.onSubscribe(SubscriptionHelper.empty());
        
        
        Px.range(0, 1_000_000).groupBy(v -> v & 1).subscribe(new Subscriber<GroupedPublisher<Integer, Integer>>() {
            @Override
            public void onSubscribe(Subscription s) {
                s.request(Long.MAX_VALUE);
            }
            
            @Override
            public void onNext(GroupedPublisher<Integer, Integer> t) {
                if (t.key() == 0) {
                    t.observeOn(ForkJoinPool.commonPool(), false, 1024).subscribe(ts1);
                } else {
                    t.observeOn(ForkJoinPool.commonPool(), false, 1024).subscribe(ts2);
                }
            }
            
            @Override
            public void onError(Throwable t) {
                ts3.onError(t);
            }
            
            @Override
            public void onComplete() {
                ts3.onComplete();
            }
        });
        
        if (!ts1.await(5, TimeUnit.SECONDS)) {
            Assert.fail("main subscriber timed out");
        }
        if (!ts2.await(5, TimeUnit.SECONDS)) {
            Assert.fail("group 0 subscriber timed out");
        }
        if (!ts3.await(5, TimeUnit.SECONDS)) {
            Assert.fail("group 1 subscriber timed out");
        }
        
        ts1.assertValueCount(500_000)
        .assertNoError()
        .assertComplete();

        ts2.assertValueCount(500_000)
        .assertNoError()
        .assertComplete();

        ts3.assertNoValues()
        .assertNoError()
        .assertComplete();

    }

    
    @Test
    public void twoGroupsConsumeWithSubscribeHide() {
        
        TestSubscriber<Integer> ts1 = new TestSubscriber<>();
        TestSubscriber<Integer> ts2 = new TestSubscriber<>();
        TestSubscriber<Integer> ts3 = new TestSubscriber<>();
        ts3.onSubscribe(SubscriptionHelper.empty());
        
        
        Px.range(0, 1_000_000).groupBy(v -> v & 1).subscribe(new Subscriber<GroupedPublisher<Integer, Integer>>() {
            @Override
            public void onSubscribe(Subscription s) {
                s.request(Long.MAX_VALUE);
            }
            
            @Override
            public void onNext(GroupedPublisher<Integer, Integer> t) {
                if (t.key() == 0) {
                    t.hide().observeOn(ForkJoinPool.commonPool()).subscribe(ts1);
                } else {
                    t.hide().observeOn(ForkJoinPool.commonPool()).subscribe(ts2);
                }
            }
            
            @Override
            public void onError(Throwable t) {
                ts3.onError(t);
            }
            
            @Override
            public void onComplete() {
                ts3.onComplete();
            }
        });
        
        ts1.await(5, TimeUnit.SECONDS);
        ts2.await(5, TimeUnit.SECONDS);
        ts3.await(5, TimeUnit.SECONDS);
        
        ts1.assertValueCount(500_000)
        .assertNoError()
        .assertComplete();

        ts2.assertValueCount(500_000)
        .assertNoError()
        .assertComplete();

        ts3.assertNoValues()
        .assertNoError()
        .assertComplete();

    }

    @Test
    public void twoGroupsFullAsyncFullHide() {
        
        TestSubscriber<Integer> ts1 = new TestSubscriber<>();
        TestSubscriber<Integer> ts2 = new TestSubscriber<>();
        TestSubscriber<Integer> ts3 = new TestSubscriber<>();
        ts3.onSubscribe(SubscriptionHelper.empty());
        
        
        Px.range(0, 1_000_000)
        .hide()
        .observeOn(ForkJoinPool.commonPool())
        .groupBy(v -> v & 1)
        .subscribe(new Subscriber<GroupedPublisher<Integer, Integer>>() {
            @Override
            public void onSubscribe(Subscription s) {
                s.request(Long.MAX_VALUE);
            }
            
            @Override
            public void onNext(GroupedPublisher<Integer, Integer> t) {
                if (t.key() == 0) {
                    t.hide().observeOn(ForkJoinPool.commonPool()).subscribe(ts1);
                } else {
                    t.hide().observeOn(ForkJoinPool.commonPool()).subscribe(ts2);
                }
            }
            
            @Override
            public void onError(Throwable t) {
                ts3.onError(t);
            }
            
            @Override
            public void onComplete() {
                ts3.onComplete();
            }
        });
        
        ts1.await(5, TimeUnit.SECONDS);
        ts2.await(5, TimeUnit.SECONDS);
        ts3.await(5, TimeUnit.SECONDS);
        
        ts1.assertValueCount(500_000)
        .assertNoError()
        .assertComplete();

        ts2.assertValueCount(500_000)
        .assertNoError()
        .assertComplete();

        ts3.assertNoValues()
        .assertNoError()
        .assertComplete();

    }

    @Test
    public void twoGroupsFullAsync() {
        
        TestSubscriber<Integer> ts1 = new TestSubscriber<>();
        TestSubscriber<Integer> ts2 = new TestSubscriber<>();
        TestSubscriber<Integer> ts3 = new TestSubscriber<>();
        ts3.onSubscribe(SubscriptionHelper.empty());
        
        
        Px.range(0, 1_000_000)
        .observeOn(ForkJoinPool.commonPool(), false, 512)
        .groupBy(v -> v & 1)
        .subscribe(new Subscriber<GroupedPublisher<Integer, Integer>>() {
            @Override
            public void onSubscribe(Subscription s) {
                s.request(Long.MAX_VALUE);
            }
            
            @Override
            public void onNext(GroupedPublisher<Integer, Integer> t) {
                if (t.key() == 0) {
                    t.observeOn(ForkJoinPool.commonPool()).subscribe(ts1);
                } else {
                    t.observeOn(ForkJoinPool.commonPool()).subscribe(ts2);
                }
            }
            
            @Override
            public void onError(Throwable t) {
                ts3.onError(t);
            }
            
            @Override
            public void onComplete() {
                ts3.onComplete();
            }
        });
        
        ts1.await(5, TimeUnit.SECONDS);
        ts2.await(5, TimeUnit.SECONDS);
        ts3.await(5, TimeUnit.SECONDS);
        
        ts1.assertValueCount(500_000)
        .assertNoError()
        .assertComplete();

        ts2.assertValueCount(500_000)
        .assertNoError()
        .assertComplete();

        ts3.assertNoValues()
        .assertNoError()
        .assertComplete();

    }

    @Test
    public void groupsCompleteAsSoonAsMainCompletes() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        
        Px.range(0, 20)
        .groupBy(i -> i % 5)
        .concatMap(v -> v, ErrorMode.IMMEDIATE, 2).subscribe(ts);
        
        ts.assertValues(0, 5, 10, 15, 1, 6, 11, 16, 2, 7, 12, 17, 3, 8, 13, 18, 4, 9, 14, 19)
        .assertComplete()
        .assertNoError();
    }

    @Test
    public void groupsCompleteAsSoonAsMainCompletesNoFusion() {
        TestSubscriber<Integer> ts = new TestSubscriber<>();
        
        Px.range(0, 20)
        .groupBy(i -> i % 5)
        .hide()
        .concatMap(v -> v, ErrorMode.IMMEDIATE, 2).subscribe(ts);
        
        ts.assertValues(0, 5, 10, 15, 1, 6, 11, 16, 2, 7, 12, 17, 3, 8, 13, 18, 4, 9, 14, 19)
        .assertComplete()
        .assertNoError();
    }

}
