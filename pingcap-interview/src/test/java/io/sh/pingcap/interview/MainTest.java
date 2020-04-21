package io.sh.pingcap.interview;


import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.File;

public class MainTest {
    String str = null;
    //@Before
    public void generateSource() {
        str = MainTest.class.getClassLoader().getResource("").getPath()+ File.separator+ "sourceDir";

        Main.generateSource(str, 1000*1000*200);
    }

    @Test
    public void testgetTopN() {
        str = MainTest.class.getClassLoader().getResource("").getPath()+ File.separator+ "sourceDir";
        long t1 = System.currentTimeMillis()/1000;
        TopnQueue queue1 = Main.getTopN(str+ File.separator+"0",str + File.separator + "split", 1024* 1024*50,100 );
        long t2 = System.currentTimeMillis()/1000;

        TopnQueue queue2 = Main.getTopN2(str+ File.separator+"0",str + File.separator + "split", 1024* 1024*50,100 );
        long t3 = System.currentTimeMillis()/1000;

        System.out.println(t2 - t1);
        System.out.println(t3 - t2);

        for (int i = 0; i < 100; i++) {
            System.out.println(queue2.poll()  + " " +  queue1.poll());
        }

    }


}
