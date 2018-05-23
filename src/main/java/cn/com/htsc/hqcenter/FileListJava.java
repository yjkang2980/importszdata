package cn.com.htsc.hqcenter;

/**
 * @author
 * @version $Id:
 * @Date created in 2017/9/19 9:27
 * @Description
 */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.net.URI;

/**
 * 遍历文件目录
 * 远程调用机器 需要 liunx 修改 /etc/hosts 添加 10.11.12.4 master
 * @author feng
 *
 */
public class FileListJava {

    public static void main(String[] args) {
        FileSystem hdfs = null;
        try{
            Configuration config = new Configuration();
            // 程序配置
            config.set("fs.default.name", "hdfs://nameservice1:9000");
            //config.set("hadoop.job.ugi", "feng,111111");
            //config.set("hadoop.tmp.dir", "/tmp/hadoop-fengClient");
            //config.set("dfs.replication", "1");
            //config.set("mapred.job.tracker", "master:9001");

            hdfs = FileSystem.get(new URI("hdfs://nameservice1:9000"),
                    config, "feng");
            Path path = new Path("/");

            iteratorShowFiles(hdfs, path);

        }catch(Exception e){
            e.printStackTrace();
        }finally{
            if(hdfs != null){
                try {
                    hdfs.closeAll();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }

    /**
     *
     * @param hdfs FileSystem 对象
     * @param path 文件路径
     */
    public static void iteratorShowFiles(FileSystem hdfs, Path path){
        try{
            if(hdfs == null || path == null){
                return;
            }
            //获取文件列表
            FileStatus[] files = hdfs.listStatus(path);

            //展示文件信息
            for (int i = 0; i < files.length; i++) {
                try{
                    if(files[i].isDirectory()){
                        System.out.println(">>>" + files[i].getPath()
                                + ", dir owner:" + files[i].getOwner());
                        //递归调用
                        iteratorShowFiles(hdfs, files[i].getPath());
                    }else if(files[i].isFile()){
                        System.out.println("   " + files[i].getPath()
                                + ", length:" + files[i].getLen()
                                + ", owner:" + files[i].getOwner());
                    }
                }catch(Exception e){
                    e.printStackTrace();
                }
            }
        }catch(Exception e){
            e.printStackTrace();
        }
    }

}