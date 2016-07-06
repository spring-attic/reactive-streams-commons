package rsc.publisher;

import org.junit.Test;

import rsc.test.TestSubscriber;

public class PublisherCharSequenceTest {

    @Test
    public void normal() {
        
        Px.characters("Hello world!")
        .test()
        .assertResult(
                (int)'H', (int)'e', (int)'l', (int)'l', 
                (int)'o', (int)' ', (int)'w', (int)'o', 
                (int)'r', (int)'l', (int)'d', (int)'!')
        ;
    }
    
    @Test
    public void normalBackpressured() {
        
        TestSubscriber<Integer> ts = Px.characters("Hello world!")
        .test(5);
        
        ts.assertValues(
                (int)'H', (int)'e', (int)'l', (int)'l', 
                (int)'o');

        ts.request(12);
        
        ts.assertValues(
                (int)'H', (int)'e', (int)'l', (int)'l', 
                (int)'o', (int)' ', (int)'w', (int)'o', 
                (int)'r', (int)'l', (int)'d', (int)'!')

        ;
    }
}
