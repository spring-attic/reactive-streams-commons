package rsc.publisher;

import java.util.*;
import java.util.stream.Collectors;

import org.junit.Test;

import rsc.test.TestSubscriber;

public class PublisherStreamCollectorTest {

    @Test
    public void collectToList() {
        Px<List<Integer>> source = Px.range(1, 5).collect(Collectors.toList());

        for (int i = 0; i < 5; i++) {
            TestSubscriber<List<Integer>> ts = new TestSubscriber<>();
            source.subscribe(ts);
    
            ts.assertValue(Arrays.asList(1, 2, 3, 4, 5))
            .assertNoError()
            .assertComplete();
        }
    }

    @Test
    public void collectToSet() {
        Px<Set<Integer>> source = Px.just(1).repeat(5).collect(Collectors.toSet());

        for (int i = 0; i < 5; i++) {
            TestSubscriber<Set<Integer>> ts = new TestSubscriber<>();
            source.subscribe(ts);
    
            ts.assertValue(Collections.singleton(1))
            .assertNoError()
            .assertComplete();
        }
    }

}
