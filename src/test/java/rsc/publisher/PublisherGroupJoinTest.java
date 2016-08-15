package rsc.publisher;

import java.util.Arrays;
import java.util.function.BiFunction;
import java.util.function.Function;

import org.junit.Test;
import rsc.processor.DirectProcessor;
import rsc.test.TestSubscriber;

public class PublisherGroupJoinTest {


        final  BiFunction<Integer, Px<Integer>, Px<Integer>> add2 =
                (t1, t2s) -> t2s.map(t2 -> t1 + t2);

        Function<Integer, Px<Integer>> just(final Px<Integer> publisher) {
            return t1 -> publisher;
        }

         <T, R> Function<T, Px<R>> just2(final Px<R> publisher) {
             return t1 -> publisher;
         }



    @Test
    public void behaveAsJoin() {
        TestSubscriber<Object> ts = new TestSubscriber<>();
        DirectProcessor<Integer> source1 = new DirectProcessor<>();
        DirectProcessor<Integer> source2 = new DirectProcessor<>();

        Px<Integer> m = source1.groupJoin(source2,
                just(Px.never()),
                just(Px.never()), add2).flatMap(t -> t);

        m.subscribe(ts);

        source1.onNext(1);
        source1.onNext(2);
        source1.onNext(4);

        source2.onNext(16);
        source2.onNext(32);
        source2.onNext(64);

        source1.onComplete();
        source2.onComplete();

        ts.assertValues(17, 18, 20, 33, 34, 36, 65, 66, 68)
          .assertComplete()
          .assertNoError();
    }

    class Person {
        final int id;
        final String name;

        public Person(int id, String name) {
            this.id = id;
            this.name = name;
        }
    }

    class PersonFruit {
        final int personId;
        final String fruit;

        public PersonFruit(int personId, String fruit) {
            this.personId = personId;
            this.fruit = fruit;
        }
    }

    class PPF {
        final Person person;
        final Px<PersonFruit> fruits;

        public PPF(Person person, Px<PersonFruit> fruits) {
            this.person = person;
            this.fruits = fruits;
        }
    }

    @Test
    public void normal1() {
        TestSubscriber<Object> ts = new TestSubscriber<>();

        Px<Person> source1 = Px.fromIterable(Arrays.asList(
                new Person(1, "Joe"),
                new Person(2, "Mike"),
                new Person(3, "Charlie")
        ));

        Px<PersonFruit> source2 = Px.fromIterable(Arrays.asList(
                new PersonFruit(1, "Strawberry"),
                new PersonFruit(1, "Apple"),
                new PersonFruit(3, "Peach")
        ));

        Px<PPF> q = source1.groupJoin(
                source2,
                just2(Px.never()),
                just2(Px. never()),
                PPF::new)
                           .doOnNext(ppf ->
                             ppf.fruits.filter(t1 -> ppf.person.id == t1.personId)
                                       .subscribe(t1 ->
                                           ts.onNext(Arrays.asList(ppf.person.name, t1
		                                       .fruit))))
		        .ignoreElements();

	    q.subscribe(ts);
        ts.assertValues(Arrays.asList("Joe", "Strawberry"), Arrays.asList("Joe", "Apple"), Arrays.asList("Charlie", "Peach"))
                .assertComplete()
                .assertNoError();
    }

    @Test
    public void leftThrows() {
        TestSubscriber<Object> ts = new TestSubscriber<>();
        DirectProcessor<Integer> source1 = new DirectProcessor<>();
        DirectProcessor<Integer> source2 = new DirectProcessor<>();

        Px<Px<Integer>> m = source1.groupJoin(source2,
                just(Px.never()),
                just(Px.never()), add2);

        m.subscribe(ts);

        source2.onNext(1);
        source1.onError(new RuntimeException("Forced failure"));

        ts.assertErrorMessage("Forced failure")
          .assertNotComplete()
          .assertNoValues();
    }

    @Test
    public void rightThrows() {
        TestSubscriber<Object> ts = new TestSubscriber<>();
        DirectProcessor<Integer> source1 = new DirectProcessor<>();
        DirectProcessor<Integer> source2 = new DirectProcessor<>();

        Px<Px<Integer>> m = source1.groupJoin(source2,
                just(Px.never()),
                just(Px.never()), add2);

        m.subscribe(ts);

        source1.onNext(1);
        source2.onError(new RuntimeException("Forced failure"));

        ts.assertErrorMessage("Forced failure")
          .assertNotComplete()
          .assertValueCount(1);
    }

    @Test
    public void leftDurationThrows() {
        TestSubscriber<Object> ts = new TestSubscriber<>();
        DirectProcessor<Integer> source1 = new DirectProcessor<>();
        DirectProcessor<Integer> source2 = new DirectProcessor<>();

        Px<Integer> duration1 = Px.error(new RuntimeException("Forced failure"));

        Px<Px<Integer>> m = source1.groupJoin(source2,
                just(duration1),
                just(Px.never()), add2);
        m.subscribe(ts);

        source1.onNext(1);

        ts.assertErrorMessage("Forced failure")
          .assertNotComplete()
          .assertNoValues();
    }

    @Test
    public void rightDurationThrows() {
        TestSubscriber<Object> ts = new TestSubscriber<>();
        DirectProcessor<Integer> source1 = new DirectProcessor<>();
        DirectProcessor<Integer> source2 = new DirectProcessor<>();

        Px<Integer> duration1 = Px.error(new RuntimeException("Forced failure"));

        Px<Px<Integer>> m = source1.groupJoin(source2,
                just(Px.never()),
                just(duration1), add2);
        m.subscribe(ts);

        source2.onNext(1);

        ts.assertErrorMessage("Forced failure")
          .assertNotComplete()
          .assertNoValues();
    }

    @Test
    public void leftDurationSelectorThrows() {
        TestSubscriber<Object> ts = new TestSubscriber<>();
        DirectProcessor<Integer> source1 = new DirectProcessor<>();
        DirectProcessor<Integer> source2 = new DirectProcessor<>();

        Function<Integer, Px<Integer>> fail = t1 -> {
            throw new RuntimeException("Forced failure");
        };

        Px<Px<Integer>> m = source1.groupJoin(source2,
                fail,
                just(Px.never()), add2);
        m.subscribe(ts);

        source1.onNext(1);

        ts.assertErrorMessage("Forced failure")
          .assertNotComplete()
          .assertNoValues();
    }

    @Test
    public void rightDurationSelectorThrows() {
        TestSubscriber<Object> ts = new TestSubscriber<>();
        DirectProcessor<Integer> source1 = new DirectProcessor<>();
        DirectProcessor<Integer> source2 = new DirectProcessor<>();

        Function<Integer, Px<Integer>> fail = t1 -> {
            throw new RuntimeException("Forced failure");
        };

        Px<Px<Integer>> m = source1.groupJoin(source2,
                just(Px.never()),
                fail, add2);
        m.subscribe(ts);

        source2.onNext(1);

        ts.assertErrorMessage("Forced failure")
          .assertNotComplete()
          .assertNoValues();
    }

    @Test
    public void resultSelectorThrows() {
        TestSubscriber<Object> ts = new TestSubscriber<>();
        DirectProcessor<Integer> source1 = new DirectProcessor<>();
        DirectProcessor<Integer> source2 = new DirectProcessor<>();

        BiFunction<Integer, Px<Integer>, Integer> fail = (t1, t2) -> {
            throw new RuntimeException("Forced failure");
        };

        Px<Integer> m = source1.groupJoin(source2,
                just(Px.never()),
                just(Px.never()), fail);
        m.subscribe(ts);

        source1.onNext(1);
        source2.onNext(2);

        ts.assertErrorMessage("Forced failure")
          .assertNotComplete()
          .assertNoValues();
    }
}
