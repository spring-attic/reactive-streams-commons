package rsc.publisher;

import org.junit.Test;

public class PublisherSumNumberTest {

    @Test
    public void normal() {
        Px.range(1, 10).sumInt().test().assertResult(55);
    }

    @Test
    public void empty() {
        Px.empty().sumInt().test().assertResult();
    }

    @Test
    public void normalLong() {
        Px.range(1, 10).map(v -> (long)v).sumLong().test().assertResult(55L);
    }

    @Test
    public void emptyLong() {
        Px.empty().sumLong().test().assertResult();
    }

    @Test
    public void minNormal() {
        Px.range(1, 10).minInt().test().assertResult(1);
    }

    @Test
    public void minEmpty() {
        Px.empty().minInt().test().assertResult();
    }

    @Test
    public void maxNormal() {
        Px.range(1, 10).maxInt().test().assertResult(10);
    }

    @Test
    public void maxEmpty() {
        Px.empty().maxInt().test().assertResult();
    }

}
