package cn.com.htsc.hqcenter;

/**
 * @author
 * @version $Id:
 * @Date created in 2017/9/13 14:36
 * @Description
 */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.*;

/**
 * Created by K0260006 on 2017/6/27.
 */
public class HdfsUtil {

    // 模型管理的根目录
    private  static String rootPath;
    private static FileSystem fs;

    static {
        ResourceBundle bundle = ResourceBundle.getBundle("option");
        System.setProperty("HADOOP_USER_NAME", bundle.getString("hadoop.user.name"));
        System.setProperty("HADOOP_USER_PASSWORD", bundle.getString("hadoop.user.password"));
        System.out.println(
                "用户名密码：" + bundle.getString("hadoop.user.name") + "--" + bundle.getString("hadoop.user.password"));

    }

    public HdfsUtil() {
        rootPath = "";
    }

    /**
     * 获取文件系统
     *
     * @return
     * @throws URISyntaxException
     * @throws IOException
     */
    public static FileSystem getFileSystem() throws URISyntaxException, IOException {
        if (fs == null) {
            //ResourceBundle bundle = ResourceBundle.getBundle("config/modelManage");
            // URI uri = new URI(bundle.getString("hdfs.url"));
            // fs = FileSystem.newInstance(new Configuration(true));
            Configuration conf = new Configuration(true);
            conf.addResource("hdfs-site.xml");
            conf.addResource("core-site.xml");
            fs = FileSystem.get(conf);
            return fs;
        } else {
            return fs;
        }
    }

    /**
     * 关闭资源
     *
     * @throws URISyntaxException
     * @throws IOException
     */
    public static void closeFileSystem() throws URISyntaxException, IOException {
        fs.close();
    }

    /**
     * 创建目录
     *
     * @param path
     * @throws IOException
     * @throws URISyntaxException
     */
    public static boolean mkDir(String path) throws IOException, URISyntaxException {
        //path =  path;
        FileSystem fileSystem;
        try {
            fileSystem = getFileSystem();
            return fileSystem.mkdirs(new Path(path));
        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;
    }

    /**
     * 判断是否存在
     *
     * @param path
     * @return
     */
    public static boolean exists(String path) throws IOException, URISyntaxException {
       // path = rootPath + path;
        FileSystem fileSystem;
        try {
            fileSystem = getFileSystem();
            return fileSystem.exists(new Path(path));
        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;
    }

    /**
     * 删除文件
     *
     * @param path
     * @return
     */
    public boolean rmFile(String path) throws IOException, URISyntaxException {
        path = rootPath + path;
        FileSystem fileSystem;
        try {
            fileSystem = getFileSystem();
            return fileSystem.delete(new Path(path), true);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;
    }

    /**
     * 上传文件
     *
     * @param src
     * @param dst
     * @return
     * @throws IOException
     * @throws URISyntaxException
     */
    public static boolean uploadFile(String src, String dst) throws IOException, URISyntaxException {
       // dst =  dst;
        FileSystem fileSystem;
        try {
            fileSystem = getFileSystem();
            // 判断目标路径
            if (!exists(dst)) {
                mkDir(dst);
            }
            fileSystem.copyFromLocalFile(true,true,new Path(src), new Path(dst));
            return true;
        } catch (URISyntaxException e) {
            e.printStackTrace();
            throw e;
        } catch (IOException e) {
            e.printStackTrace();
            throw e;
        }
    }

    /**
     * 遍历目录，使用FileSystem的listStatus(path) 如果要查看file状态，使用FileStatus对象
     *
     * @throws Exception
     */
    public static List<Map<String, Object>> list(String path) throws IOException, URISyntaxException {
        path = rootPath + path;
        FileSystem fileSystem;
        fileSystem = getFileSystem();
        FileStatus[] listStatus = fileSystem.listStatus(new Path(path));
        List<Map<String, Object>> files = new ArrayList<Map<String, Object>>();
        for (FileStatus fileStatus : listStatus) {
            String fileName = fileStatus.getPath().getName();
            String filePath = fileStatus.getPath().toUri().getPath();
            boolean isFile = fileStatus.isFile();
            boolean isDirectory = fileStatus.isDirectory();
            Map<String, Object> file = new HashMap<String, Object>();
            file.put("fileName", fileName);
            file.put("filePath", filePath);
            file.put("isFile", isFile);
            file.put("isDirectory", isDirectory);
            files.add(file);
        }
        return files;
    }

    /**
     * 测试方法
     *
     * @param args
     */
    public static void main(String[] args) throws IOException, URISyntaxException {
        //HdfsUtil hdfsUtil = new HdfsUtil();
        // System.err.println(hdfsUtil.mkDir("tmp_file"));
        // System.err.println(hdfsUtil.exists("tmp_file"));
        // hdfsUtil.uploadFile("D:/Users/K0260005\\Desktop\\1.txt", "tmp_file");
        // List<Map<String, Object>> list = hdfsUtil.list("tmp_file");
        // hdfsUtil.rmFile("user");
        // System.err.println("end");

       // System.out.println(hdfsUtil.exists("/user/crmpicturewr/nginx.conf"));
       // System.out.println(new File("D:\\tmp\\mddata\\2016\\08\\31\\am_hq_index.txt").exists());
      uploadFile("C:\\tmp.log","hdfs://nameservice1/user/u010571/data");
    }
}
