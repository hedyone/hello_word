package io.sh.pingcap.interview;


import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class TopnQueueTest {


    @Test
    public void push() {
        TopnQueue queue  = new TopnQueue(5);
        Map<String ,Long> map = new HashMap<String, Long>();
        map.put("a", 1L);
        map.put("b", 2L);
        map.put("c", 3L);
        map.put("d", 4L);
        map.put("e", 5L);
        map.put("f", 6L);
        map.put("g", 8L);
        map.put("h", 9L);
        for (Map.Entry<String,Long> entry: map.entrySet()) {
            queue.push(entry);
        }

        for (int i = 0; i < 5 ; i++ ) {
           System.out.println( queue.poll() + " ");
        }
    }
}
