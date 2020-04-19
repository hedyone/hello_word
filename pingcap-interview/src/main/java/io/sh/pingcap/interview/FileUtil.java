package io.sh.pingcap.interview;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FileUtil {

    private  static  HashMap<String, Long> cacheMap = new HashMap<String, Long>();

    /**
     * 单线程分割文件，这里使用单线程处理，可以使用更大的内存hash表，从而获得更高的预聚合，减少了输出到文件的数据量；
     * @param sourceFilePath
     * @param aviableMemerySize
     * @param destDirName
     */
    public static void splitFile(String sourceFilePath, long aviableMemerySize, String destDirName) {
        File  soureFile = new File(sourceFilePath);
        String sourceFileParent = soureFile.getParent();
        String fileSplitDir =  destDirName;
        File destDir = new File(fileSplitDir);
        if (destDir.exists()) {
            destDir.delete();
        }
        destDir.mkdirs();

        //计算分片文件数量
        long fileLength = soureFile.length();
        System.out.println( sourceFilePath + " split to " + fileLength +" files");
        //预留10%的空间给应用程序
        final double hashMapLimit =  aviableMemerySize*0.9;
        int fileNumber = (int) (Math.ceil((double) fileLength/ (double) hashMapLimit));
        //记录hash表占用的内存大小
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
                    //计算每条记录占用的内存大小= url的长度 + length (long) + hashcoe(int型) + 对象头 + key对象引用
                    //TODO 自定义hashtable, 使用数组存放hashtable,避免对象头，和指针引用造成的空间占用
                    hashMapSize += (long) url.length() + 8L + 4L + 16L + 4L;

                } else if (cacheMap.get(url) != null && hashMapSize < hashMapLimit) {
                    //这里只是更新map中数据，无需更新内存占用
                    cacheMap.put(url, cacheMap.get(url) + count);
                }

                if (hashMapSize >= hashMapLimit) {
                    for (Map.Entry<String, Long> entry : cacheMap.entrySet()) {
                        int number = Math.abs(entry.getKey().hashCode()% fileNumber);
                        WriteToFile(entry.getKey(), entry.getValue(), bws[number]);
                    }
                    //写出后清空缓存信息
                    cacheMap.clear();
                    hashMapSize = 0;
                }
            }
            //写出最后一部分数据，
            // TODO 这里有一个优化点，如果在遍历完源文件后，内存空间始终够用,即url的基数比较低的情况，是可以直接进行统计的
            for (Map.Entry<String, Long> entry : cacheMap.entrySet()) {
                int number = Math.abs(entry.getKey().hashCode()% fileNumber);
                WriteToFile(entry.getKey(), entry.getValue(), bws[number]);
            }
            //写出后清空缓存信息
            cacheMap.clear();
            hashMapSize = 0;


        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            closeWriters(list);
            closeReaders(reader);
        }

        //每次按分割完文件后，可能存在数据倾斜，某个split文件特别大，需要再次分割
        //这里使用递归完成文件的分割
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
                splitFile(fileSplitDir + File.separator + splitFiles[i].getName(), aviableMemerySize,
                        splitFiles[i].getAbsolutePath()+ "__");
                //这里有可能删除文件失败，通过改名，废弃掉被二次划分的的文件文件
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

    /**
     * 将单个文件聚合排序
     * @param filename
     * @return
     */
    public static Map sortFile(String filename) {
        cacheMap.clear();
        BufferedReader reader = getReader(filename);
        String line = null;
        try {
            while ((line = reader.readLine()) != null) {
                String word[] = line.split(" ");
                String url = word[0];
                long count = Long.parseLong(word[1]);

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
     * 获取一个map中的topn
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
                Map<String, Long> map = sortFile(splitFiles[i].getAbsolutePath());
                queue = getTopNfromMap(map, queue);
            } else {
                getTopN(splitFiles[i].getAbsolutePath(), queue);
            }
        }
        return queue;
    }


    /**
     * 获取一组writer
     * @param fileSplitDir
     * @param fileNumber
     * @return
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
     * 关闭一组writer
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
