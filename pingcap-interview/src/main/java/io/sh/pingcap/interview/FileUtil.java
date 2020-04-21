package io.sh.pingcap.interview;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class FileUtil {

    private  static  HashMap<String, Long> cacheMap = new HashMap<String, Long>();

    /**
     * split file with single thread
     * @param sourceFilePath
     * @param aviableMemerySize
     * @param destDirName
     */
    public static void splitFile(String sourceFilePath, long aviableMemerySize, String destDirName) {

        File  soureFile = new File(sourceFilePath);
        String fileSplitDir =  destDirName;
        File destDir = new File(fileSplitDir);
        if (destDir.exists()) {
            destDir.delete();
        }
        destDir.mkdirs();

        long fileLength = soureFile.length();
        // 20% memery for run time
        final double hashMapLimit =  aviableMemerySize*0.8;
        int fileNumber = (int) (Math.ceil((double) fileLength/ (double) hashMapLimit));
        System.out.println( sourceFilePath + " split to " + fileNumber +" files");
        //use to record size of hashMap
        long hashMapSize = 0L;

        List < Closeable[]> list  = getBufferWriters(fileSplitDir, fileNumber, "split_");
        BufferedWriter bws[] = (BufferedWriter[]) list.get(0);

        BufferedReader reader = getReader(sourceFilePath);
        String line = null;

        long start = System.currentTimeMillis()/1000;
        try {
            while ((line = reader.readLine()) != null) {
                if (line.length() == 0) {
                    continue;
                }
                String word[] = line.split(" ");
                String url = word[0];
                long count = 1;
                int urlLength = url.length();

                if (word.length > 1) {
                    count = Long.parseLong(word[1]);
                }
                if (cacheMap.get(url) == null && hashMapSize < hashMapLimit) {
                    cacheMap.put(url, count);
                    //size if record = length(url) + length (long) + length(hashcode) + length(markword) + length(referance)
                    //TODO 自定义hashtable, 使用数组存放hashtable,避免对象头，和指针引用造成的空间占用
                    hashMapSize += (long) url.length() + 8L + 4L + 16L + 4L;

                } else if (cacheMap.get(url) != null && hashMapSize < hashMapLimit) {
                    cacheMap.put(url, cacheMap.get(url) + count);
                }

                if (hashMapSize >= hashMapLimit) {
                    for (Map.Entry<String, Long> entry : cacheMap.entrySet()) {
                        int number = Math.abs(entry.getKey().hashCode()% fileNumber);
                        WriteToFile(entry.getKey(), entry.getValue(), bws[number]);
                    }
                    //clear the map after write it`s content
                    cacheMap.clear();
                    hashMapSize = 0;
                }
            }
            //
            // TODO map be we can add a flag to infer wheather the map can hold all record
            for (Map.Entry<String, Long> entry : cacheMap.entrySet()) {
                int number = Math.abs(entry.getKey().hashCode()% fileNumber);
                WriteToFile(entry.getKey(), entry.getValue(), bws[number]);
            }

            cacheMap.clear();
            hashMapSize = 0;


        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            closeWriters(list);
            closeReaders(reader);
        }

        // after split the sourcefile , there may be data skew, the splits still unmatch
        // the memery size ,so we split file recursively until it macth the size;
        File file = new File(fileSplitDir);
        FilenameFilter filter = new FilenameFilter() {
            public boolean accept(File dir, String name) {
                if (name.endsWith(".old")) {
                    return false;
                } else {
                    return true;
                }
            }
        };

        File splitFiles[] = file.listFiles(filter);

        for (int i = 0; i <splitFiles.length ; i++) {
            if (splitFiles[i].length() > hashMapLimit && splitFiles[i].isFile() ) {
                splitFile(splitFiles[i].getAbsolutePath(), aviableMemerySize,
                        splitFiles[i].getAbsolutePath()+ "__");
                //in case of delete file failed , rename file to avoid error result
                if (!splitFiles[i].delete()) {
                   try {
                       splitFiles[i].renameTo(new File(splitFiles[i].getAbsoluteFile() + ".old"));
                   } catch (Exception e) {
                       e.printStackTrace();
                   }
                }
            }

        }
        long end = System.currentTimeMillis() / 1000;
        System.out.print(" split file completed ,spend : " );
        System.out.println(end - start );

    }


    //produceer-consumer mode to deal with big file i/o, a single thread for read, and a single thread for write
    private static Map<String, Long> mapReadin = new HashMap();
    private static Map<String,Long> mapWriteOut = new HashMap();
    private static Map tmp ;
    private static volatile boolean isReadin;
    private static volatile boolean isWriteout;
    private static volatile boolean completed;
    private static ReentrantLock lock = new ReentrantLock();
    private static Condition writeCondition = lock.newCondition();
    private static Condition readCondition = lock.newCondition();

    /**
     * a single thread for write intermedia result to splited file
     */
    public static void fileSplitWriter(String sourceFilePath, long aviableMemerySize, String destDirName ) {
        File  soureFile = new File(sourceFilePath);
        String fileSplitDir =  destDirName;
        File destDir = new File(fileSplitDir);
        if (destDir.exists()) {
            destDir.delete();
        }
        destDir.mkdirs();

        long fileLength = soureFile.length();
        // 20% memery for run time
        final double hashMapLimit =  aviableMemerySize*0.8;
        int fileNumber = (int) (Math.ceil((double) fileLength/ (double) hashMapLimit));
        System.out.println( sourceFilePath + " split to " + fileNumber +" files");
        //use to record size of hashMap
        long hashMapSize = 0L;

        List < Closeable[]> list  = getBufferWriters(fileSplitDir, fileNumber, "split_");
        BufferedWriter bws[] = (BufferedWriter[]) list.get(0);
        while (true) {
            lock.lock();
            try {
                while (mapWriteOut.isEmpty()) {
                    if (isReadin) {
                        writeCondition.await();
                    }
                    readCondition.signal();
                    if (completed && mapWriteOut.isEmpty()) {
                        closeWriters(list);
                        return;
                    }

                }
            } catch (InterruptedException e) {
                e.printStackTrace();
                throw  new RuntimeException("write thread Interrupted");
            }

            finally {
                lock.unlock();
            }
            isWriteout = true;
            System.out.println("mapWriteOut size : "+ mapWriteOut.size() );

            for (Map.Entry<String, Long> entry: mapWriteOut.entrySet()) {
                int number = Math.abs(entry.getKey().hashCode()% fileNumber);
                WriteToFile(entry.getKey(), entry.getValue(), bws[number]);
            }
            mapWriteOut.clear();
            isWriteout = false;
        }

    }

    /**
     * a single thread reader that read record from source file;
     * @param sourceFilePath
     * @param aviableMemerySize
     */
    public static  void fileSplitReader(String sourceFilePath, long aviableMemerySize) {
        File  soureFile = new File(sourceFilePath);


        long fileLength = soureFile.length();
        // 20% memery for run time
        final double hashMapLimit =  Math.ceil(aviableMemerySize*0.8);
        //use to record size of hashMap
        long hashMapSize = 0L;

        BufferedReader reader = getReader(sourceFilePath);
        String line = null;

        while (true) {
            lock.lock();
            try {
                while (!mapReadin.isEmpty()) {
                    if (isWriteout) {
                        readCondition.await();
                    }
                    tmp = mapWriteOut;
                    mapWriteOut = mapReadin;
                    mapReadin = tmp;
                    writeCondition.signal();
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
                throw new RuntimeException("read thread been Interrupted");
            } finally {
                lock.unlock();
            }
            if(completed) {
                break;
            }

            try {
                while ((line = reader.readLine())!= null){
                    isReadin = true;
                    if (line.length() == 0) {
                        continue;
                    }
                    String word[] = line.split(" ");
                    String url = word[0];
                    long count = 1;
                    int urlLength = url.length();

                    if (word.length > 1) {
                        count = Long.parseLong(word[1]);
                    }
                    if (mapReadin.get(url) == null && hashMapSize < hashMapLimit) {
                        mapReadin.put(url, count);
                        hashMapSize += (long) url.length() + 8L + 4L + 16L + 4L;

                    } else if (mapReadin.get(url) != null && hashMapSize < hashMapLimit) {
                        mapReadin.put(url, mapReadin.get(url) + count);
                    }

                    if (hashMapSize >= hashMapLimit) {
                        hashMapSize = 0L;
                        isReadin = false;
                        break;
                    }
                }

                if (line == null) {
                    isReadin = false;
                    completed = true;
                    continue;
                }
            } catch (Exception e) {
                e.printStackTrace();
            }

        }
    }

    public static void splitFileWithMultiThread(final String sourceFilePath, final long aviableMemerySize,
                                         final String destDirName) {
        Runnable writer = new Runnable() {
            public void run() {
                fileSplitWriter(sourceFilePath,  aviableMemerySize,  destDirName);
            }
        };
        Thread t = new Thread(writer);
        t.start();
        fileSplitReader(sourceFilePath,  aviableMemerySize);
        try {
            t.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
            throw  new RuntimeException("wait reader interrupted");
        }
        // deal with data skew
        File file = new File(destDirName);
        FilenameFilter filter = new FilenameFilter() {
            public boolean accept(File dir, String name) {
                if (name.endsWith(".old")) {
                    return false;
                } else {
                    return true;
                }
            }
        };

        File splitFiles[] = file.listFiles(filter);
        final double hashMapLimit =  aviableMemerySize*0.8;

        for (int i = 0; i <splitFiles.length ; i++) {
            if (splitFiles[i].length() > hashMapLimit && splitFiles[i].isFile() ) {
                splitFile(splitFiles[i].getAbsolutePath(), aviableMemerySize,
                        splitFiles[i].getAbsolutePath()+ "__");
                //in case of delete file failed , rename file to avoid error result
                if (!splitFiles[i].delete()) {
                    try {
                        splitFiles[i].renameTo(new File(splitFiles[i].getAbsoluteFile() + ".old"));
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }

        }

    }

    /**
     * merge duplicate key
     * @param filename
     * @return  map with sort key
     */
    public static Map aggKey(String filename) {
        cacheMap.clear();
        BufferedReader reader = getReader(filename);
        String line = null;
        try {
            while ((line = reader.readLine()) != null) {
                String word[] = line.split(" ");
                String url = word[0];
                long count = 1;
                if (word.length == 2) {
                     count = Long.parseLong(word[1]);
                }

                if (cacheMap.get(url) == null) {
                    cacheMap.put(url, count);

                } else if (cacheMap.get(url) != null) {
                    cacheMap.put(url, cacheMap.get(url) + count);
                }

            }
        }  catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("read file error");
        } finally {
            closeReaders(reader);
            return cacheMap;
        }

    }

    /**
     *
     * @param map
     * @param queue
     * @return
     */
    public static TopnQueue getTopNfromMap(Map<String , Long> map, TopnQueue queue) {
        for (Map.Entry<String, Long> entry: map.entrySet()) {
            queue.push(entry);
        }
        return queue;
    }

    public static TopnQueue getTopN(String splitDir, TopnQueue queue) {
        FilenameFilter filter = new FilenameFilter() {
            public boolean accept(File dir, String name) {
                if (name.endsWith(".old")) {
                    return false;
                } else {
                    return true;
                }
            }
        };

        File splitFiles[] = new File(splitDir).listFiles(filter) ;
        for (int i = 0; i < splitFiles.length; i++) {
            if (splitFiles[i].isFile()) {
                Map<String, Long> map = aggKey(splitFiles[i].getAbsolutePath());
                queue = getTopNfromMap(map, queue);
            } else {
                getTopN(splitFiles[i].getAbsolutePath(), queue);
            }
        }
        return queue;
    }


    /**
     * get the split file fd
     * @param fileSplitDir  split dir
     * @param fileNumber number of file
     * @return list of file fd
     */
    public static List<Closeable[]> getBufferWriters(String fileSplitDir, int fileNumber, String dirFlag) {

        File parent = new File(fileSplitDir);
        parent.mkdirs();
        List< Closeable[]> list= new ArrayList<Closeable[]>();
        File[] files = new File[fileNumber];
        FileOutputStream[] fops = new FileOutputStream[fileNumber];
        OutputStreamWriter[] writers = new OutputStreamWriter[fileNumber];
        //使用缓冲writer,提高刷盘性能
        BufferedWriter[] bufferWriters = new BufferedWriter[fileNumber];

        try {
            for (int i = 0; i < fileNumber; i++) {
                String str = fileSplitDir+ File.separator + dirFlag + i;
                System.out.println(str);
                files[i] = new File(str);
                files[i].createNewFile();
                fops[i] = new FileOutputStream(files[i], false);
                writers[i] = new OutputStreamWriter(fops[i], "UTF-8");
                bufferWriters[i] = new BufferedWriter(writers[i], 1 * 1024 * 1024);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        list.add( bufferWriters);
        list.add( writers);
        list.add( fops);
        return list;
    }

    /**
     * close a list of file fd
     * @param list
     */
    public static void closeWriters(List< Closeable[]> list) {
        for (Closeable[] closeables : list) {
            for (Closeable closeable: closeables) {
                try {
                    closeable.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }

    /**
     * close a list of file fd
     * @param readers
     */
    public static void closeReaders(BufferedReader ... readers) {
        for (int i = 0; i < readers.length ; i++) {
            if (readers[i] != null) {
                try {
                    readers[i].close();
                } catch (Exception e) {
                    e.printStackTrace();
                }

            }

        }
    }

    public static BufferedReader getReader(String sourceFilePath) {
        FileInputStream inputStream = null;
        BufferedInputStream bis = null;
        BufferedReader reader = null;

        try {
            inputStream = new FileInputStream(sourceFilePath);
            bis = new BufferedInputStream(inputStream);
            reader = new BufferedReader(new InputStreamReader(bis, "utf-8"), 1 * 1024 * 1024);
            return reader;

        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("sourceFilePath is null");
        }
    }

    public static void WriteToFile(String key, long value, BufferedWriter bw) {
        try {
            bw.append(key + " " + value);
            bw.newLine();
        } catch (Exception e) {
            e.printStackTrace();
            throw  new RuntimeException("get exception when write record");
        }


    }

    public static void deleteFile(String dir) {
        File file = new File(dir);
        if (file.exists() && file.isFile()) {
            file.delete();
            return;
        }

        if (file.exists() && file.isDirectory()) {
            String children[] = file.list();
            for (int i = 0; i < children.length; i++) {
                deleteFile(children[i]);
            }
        }
    }

}
