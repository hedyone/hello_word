package io.sh.pingcap.interview;

import java.util.Comparator;
import java.util.Map;
import java.util.PriorityQueue;

/**
 * 封装优先队列，构造最小堆，
 * 队列中只保存最大的n个元素
 */
public class TopnQueue {
    private int topN;
    private PriorityQueue<Map.Entry<String, Long>> queue;

    public TopnQueue(int topN) {
        if (topN <= 0 ) {
            throw new RuntimeException("ill legal argument");
        }
        this.topN = topN;
        this.queue = new PriorityQueue<Map.Entry<String, Long>>(topN, new Comparator<Map.Entry<String, Long>>() {
            public int compare(Map.Entry<String, Long> o1, Map.Entry<String, Long> o2) {
                long re =  o1.getValue() - o2.getValue();
                if (re == 0) {
                    return 0;
                } else if (re > 0) {
                    return 1;
                } else {
                    return -1;
                }
            }
        });
    }

    public void push(Map.Entry<String, Long> entry) {
        if (   queue.size() <  topN) {
            queue.add(entry);
        } else {
            // 堆满了后，每次添加元素时，与堆顶做比较，如果小于堆顶元素，丢弃，如果大于
            //堆顶元素，弹出堆顶元素，然后将entry添加到最小堆中
            Map.Entry<String, Long> peek = queue.peek();
            if (entry.getValue() - peek.getValue() > 0) {
                queue.poll();
                queue.add(entry);
            }
        }
    }


    public Map.Entry  poll() {
        if (queue.size() == 0) {
            return null;
        } else {
            return queue.poll();
        }
    }

}
