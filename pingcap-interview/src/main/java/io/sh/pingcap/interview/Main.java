package io.sh.pingcap.interview;

import javax.xml.transform.Source;
import java.io.BufferedWriter;
import java.io.Closeable;
import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static io.sh.pingcap.interview.FileUtil.deleteFile;

public class Main {
    public static void generateSource(String sourceFilename, long count) {
        String urls[] = {"http://ab", "http://ac", "http://ad", "http://ae", "http://af", "http://ag", "http://ah",
                "http://ai", "http://aj", "http://ak", "http://al",};
        Random random = new Random();
        File sourceFile = new File(sourceFilename);
        if (sourceFile.exists()) {
            sourceFile.delete();
        }
        List<Closeable[]> list = FileUtil.getBufferWriters(sourceFilename, 1, "");
        BufferedWriter[] writers = (BufferedWriter[]) list.get(0);
        BufferedWriter writer = writers[0];
        long startTime = System.currentTimeMillis();
        try {
            for (int i = 0; i < count; i++) {
                int num = random.nextInt(urls.length - 1);
                String string = urls[num] + random.nextInt()%100000;
                writer.append(string);
                writer.newLine();

            }
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("generate file failed");
        } finally {
            FileUtil.closeWriters(list);
        }

        long time = (System.currentTimeMillis() - startTime ) / 1000;
        System.out.println("generate test file success, spend " );
        System.out.print(time);
        System.out.print("  second");

    }

    public static TopnQueue getTopN(String sourceFile, String splitDir, long aviableMemerySize , int topn) {
        FileUtil.splitFile(sourceFile, aviableMemerySize, splitDir);
        TopnQueue topnQueue = new TopnQueue(topn);
        TopnQueue  queue= FileUtil.getTopN(splitDir, topnQueue);
        return queue;
    }

    /**
     * 程序主入口
     * 参数：
     *     args[0] 待计算的大文件名称（全路径）
     *     args[1] 存放中间结果的临时目录（全路径）
     *     args[2] 可用的内存
     *     args[3] 取前多少个元素
     * @param args
     */
    public static void main(String args[]) {
        if (args.length != 4) {
            System.out.println("please input right param ");
            System.out.println("paramlist sourceFilePath tmpFileDir aviableMemerySize topn");
            return;
        }
        TopnQueue queue = getTopN(args[0], args[1], Long.parseLong(args[2]), Integer.parseInt(args[3]));
        for (int i = 0; i < Integer.parseInt(args[3]); i++) {
            System.out.println(queue.poll());

        }

        deleteFile(args[1]);
    }
}
