package io.sh.pingcap.interview;


import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.File;

public class MainTest {
    String str = null;
    @Before
    public void generateSource() {
        str = MainTest.class.getClassLoader().getResource("").getPath()+ File.separator+ "sourceDir";

        //Main.generateSource(str, 1000*1000*200);
    }

    @Test
    public void testgetTopN() {

        TopnQueue queue = Main.getTopN(str+ File.separator+"0",str + File.separator + "split", 1024* 1024*50,100 );
        for (int i = 0 ; i < 100; i++) {
            System.out.println(queue.poll());
        }
        FileUtil.deleteFile(str);
    }


}
