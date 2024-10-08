import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

public class hdfs {
    /**
     * 上传文件
     *
     * @param localPath 本地路径
     * @param hdfsPath  hdfs路径
     * @throws Exception 异常
     */
    public void uploadFile(String localPath, String hdfsPath) throws Exception {
        //设置hdfs的地址
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://node1:8020");
        //通过cxx用户链接
        System.setProperty("HADOOP_USER_NAME", "cxx");
        FileSystem fs = FileSystem.get(conf);
        //上传文件
        fs.copyFromLocalFile(new Path(localPath), new Path(hdfsPath));
        //关闭链接
        fs.close();
    }

    /**
     * 写入文件内容
     *
     * @param hdfsPath hdfs路径
     * @param content  内容
     * @throws Exception 异常
     */
    public void writeFile(String hdfsPath, String content) throws Exception {
        //设置hdfs的地址
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://node1:8020");
        //通过cxx用户链接
        System.setProperty("HADOOP_USER_NAME", "cxx");
        FileSystem fs = FileSystem.get(conf);
        FSDataOutputStream outputStream = fs.create(new Path(hdfsPath));
        outputStream.write(content.getBytes());
        outputStream.close();
        //关闭链接
        fs.close();
    }

    /**
     * 读取文件内容
     *
     * @param hdfsPath hdfs路径
     * @throws Exception 异常
     */
    public void readFile(String hdfsPath) throws Exception {
        //设置hdfs的地址
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://node1:8020");
        //通过cxx用户链接
        System.setProperty("HADOOP_USER_NAME", "cxx");
        FileSystem fs = FileSystem.get(conf);
        FSDataInputStream inputStream = fs.open(new Path(hdfsPath));
        String str = null;
        while ((str = inputStream.readLine()) != null) {
            System.out.println(str);
        }
        inputStream.close();
        //读取文件
        fs.close();
    }

    /**
     * 重命名文件
     *
     * @param oldPath 旧路径
     * @param newPath 新路径
     * @throws Exception 异常
     */
    public void renameFile(String oldPath, String newPath) throws Exception {
        //设置hdfs的地址
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://node1:8020");
        //通过cxx用户链接
        System.setProperty("HADOOP_USER_NAME", "cxx");
        FileSystem fs = FileSystem.get(conf);
        //重命名
        fs.rename(new Path(oldPath), new Path(newPath));
        fs.close();
    }

    /**
     * 删除文件
     *
     * @param hdfsPath hdfs路径
     * @throws Exception 异常
     */
    public void deleteFile(String hdfsPath) throws Exception {
        //设置hdfs的地址
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://node1:8020");
        //通过cxx用户链接
        System.setProperty("HADOOP_USER_NAME", "cxx");
        FileSystem fs = FileSystem.get(conf);
        //删除文件
        fs.delete(new Path(hdfsPath), false);
        fs.close();
    }

    /**
     * 创建目录
     *
     * @param hdfsPath hdfs路径
     * @throws Exception 异常
     */
    public void mkDir(String hdfsPath) throws Exception {
        //设置hdfs的地址
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://node1:8020");
        //通过cxx用户链接
        System.setProperty("HADOOP_USER_NAME", "cxx");
        FileSystem fs = FileSystem.get(conf);
        //创建文件夹
        fs.mkdirs(new Path(hdfsPath));
        //关闭链接
        fs.close();
    }

    /**
     * 删除目录
     *
     * @param hdfsPath hdfs路径
     * @throws Exception 异常
     */
    public void deleteDir(String hdfsPath) throws Exception {
        //设置hdfs的地址
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://node1:8020");
        //通过cxx用户链接
        System.setProperty("HADOOP_USER_NAME", "cxx");
        FileSystem fs = FileSystem.get(conf);
        //删除文件
        fs.delete(new Path(hdfsPath), true);
        fs.close();
    }

    /**
     * 列出文件
     *
     * @param hdfsPath hdfs路径
     * @throws Exception 异常
     */
    public void listFile(String hdfsPath) throws Exception {
        //设置hdfs的地址
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://node1:8020");
        //通过cxx用户链接
        System.setProperty("HADOOP_USER_NAME", "cxx");
        FileSystem fs = FileSystem.get(conf);
        //列出文件
        RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path(hdfsPath), true);
        while (listFiles.hasNext()) {
            LocatedFileStatus fileStatus = listFiles.next();
            System.out.println("文件名" + fileStatus.getPath().getName());
            System.out.println("文件块大小" + fileStatus.getBlockSize());
            System.out.println("文件权限" + fileStatus.getPermission());
            System.out.println("文件内容长度" + fileStatus.getLen());
            System.out.println("======================================================");
        }
        fs.close();
    }
}
