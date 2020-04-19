package io.sh.pingcap.interview;


import org.junit.Test;

import java.io.File;
import java.io.IOException;

public class FileUtilTest {


    @Test
    public void splitFile() {
        FileUtil.splitFile("sourcefile", 1024 * 1024 * 20,"/opt/test/split" );
    }
}
