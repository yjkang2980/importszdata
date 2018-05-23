package cn.com.htsc.hqcenter.dataretrive;

import org.apache.commons.net.ftp.FTPClient;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.nio.charset.Charset;

/**
 * @author 010571
 * @version $Id:
 * @Date created in 2018/4/13 15:14
 * @Description
 */
public class UPloadToFtp {

    public static  FTPClient getFtpClient(){
        FTPClient  ftp = new FTPClient();
        try {
            ftp.connect("168.8.2.60", 21);
            System.out.println(" 登陆："+ftp.login("htzq", "htzq"));
          //  ftp.setCharset(Charset.forName("UTF-8"));
            ftp.setControlEncoding("UTF-8");

        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return ftp;
    }


    public static void main(String[] args) throws Exception{
        URI uri = new URI("hdfs://nameservice1");
        Configuration conf = new Configuration();
        FileSystem fileSystem = FileSystem.get(uri, conf);
        InputStream in = fileSystem.open(new Path("hdfs://nameservice1/user/liwq/1.txt"));
        uploadToFtp(in,"/hjp/20180412/index/statistic","text.txt");
    }


    public static void uploadToFtp(InputStream in,String path,String filename) throws Exception{

        System.out.println("上传开始："+path+"--"+filename);
        FTPClient ftp=getFtpClient();

       mkdirs(ftp,path);


        //cd(ftp,path);
          ftp.changeToParentDirectory();
        ftp.changeWorkingDirectory(path);
        ftp.setBufferSize(1024);
        //getFtpClient().setControlEncoding("GBK");
        ftp.setFileType(FTPClient.BINARY_FILE_TYPE);

        boolean stat = ftp.storeFile(filename,in);
        in.close();  //关闭输入流

        ftp.logout();

    }

    /**
     * 循环切换目录
     * @param dir
     * @return
     */
    public static boolean cd( FTPClient ftp,String dir){
        boolean stat = true;
        try {
            String[] dirs = dir.split("/");
            if(dirs.length == 0){
                return  ftp.changeWorkingDirectory(dir);
            }

            stat =  ftp.changeToParentDirectory();
            for(String dirss : dirs){
                stat = stat &&  ftp.changeWorkingDirectory(dirss);
               System.out.println("changing into DIR:"+dirss);
            }

            stat = true;
        } catch (IOException e) {
            stat = false;
        }
        return stat;
    }



    /***
     * 创建多个层级目录
     * @param dir dong/zzz/ddd/ewv
     * @return
     */
    public static  boolean mkdirs( FTPClient ftp,String dir){
        String[] dirs = dir.split("/");
        if(dirs.length == 0){
            return false;
        }
        boolean stat = false;
        try {
            ftp.changeToParentDirectory();
            for(String dirss : dirs){
               System.out.println("成功创建文件夹："+dirss+"---"+ftp.makeDirectory(dirss));
                ftp.changeWorkingDirectory(dirss);
            }

            ftp.changeToParentDirectory();
            stat = true;
        } catch (IOException e) {
            stat = false;
        }
        return stat;
    }

}
