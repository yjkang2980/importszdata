package cn.com.htsc.hqcenter;

import org.apache.log4j.Logger;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.regex.Pattern;

public class FileTransferUtil {

    private static final Logger LOGGER = Logger.getLogger(FileTransferUtil.class);

    private static String filePath = "";
    private static String zhongzhuanDir = "";
    private static String startFile = "";
    private static String endFile = "";
    private static List<File> filelist = new ArrayList<File>();
    private static String snapnewPath = "hdfs://nameservice1/user/u010571/snapshotnew";
    private static String snapoldPath = "hdfs://nameservice1/user/u010571/snapshotold";
    private static String snaplevelnewPath = "hdfs://nameservice1/user/u010571/snapshotlevelnew";
    private static String snapleveloldPath = "hdfs://nameservice1/user/u010571/snapshotlevelold";
    private static String orderoldPath = "hdfs://nameservice1/user/u010571/orderold";
    private static String ordernewPath = "hdfs://nameservice1/user/u010571/ordernew";
    private static String tradeoldPath = "hdfs://nameservice1/user/u010571/tradeold";
    private static String tradenewPath = "hdfs://nameservice1/user/u010571/tradenew";
    private static String indexoldPath = "hdfs://nameservice1/user/u010571/indexold";
    private static String indexnewPath = "hdfs://nameservice1/user/u010571/indexnew";
    private static String stockstatusoldPath = "hdfs://nameservice1/user/u010571/stockstatusold";
    private static String stockstatusnewPath = "hdfs://nameservice1/user/u010571/stockstatusnew";


    //private static


    static {
        ResourceBundle bundle = ResourceBundle.getBundle("option");
        filePath = bundle.getString("path");
        zhongzhuanDir = bundle.getString("destDir");
        startFile = bundle.getString("startFile");
        endFile = bundle.getString("endFile");
    }

    public static void getOrderedDescFiles() {
        Collections.sort(filelist, new Comparator<File>() {
            public int compare(File o1, File o2) {
                return o2.getAbsolutePath().compareTo(o1.getAbsolutePath());
            }
        });

    }

    public static List<File> getFileList(String strPath) {
        File dir = new File(strPath);
        File[] files = dir.listFiles(); // 该文件目录下文件全部放入数组
        if (files != null) {
            for (int i = 0; i < files.length; i++) {
                String fileName = files[i].getName();
                if (files[i].isDirectory()) { // 判断是文件还是文件夹
                    getFileList(files[i].getAbsolutePath()); // 获取文件绝对路径
                } else if (files[i].isFile() && !files[i].getName().endsWith(".dll")) {
                    String strFileName = files[i].getAbsolutePath();
                    //   System.out.println("---" + strFileName);
                    filelist.add(files[i]);
                } else {
                    continue;
                }
            }

        }
        return filelist;
    }

    @Test
    public void testGetFile(){
        getFileList(filePath);
        getOrderedDescFiles();
        for (File f : filelist) {
           // LOGGER.info("directory" + f.getPath());

            if (f.getParentFile().getAbsolutePath().compareTo(startFile) > 0 && f.getParentFile().getAbsolutePath().compareTo(endFile) <= 0) {

                    LOGGER.info("directory" + f.getPath());
            }
        }
    }


    public static void main(String[] args) {


        ExecutorService exe = Executors.newFixedThreadPool(10);

        getFileList(filePath);
        getOrderedDescFiles();
        //List<File> lfs = new ArrayList<File>();
        List<File> lfs = new ArrayList<File>();

        int fileNum = 0;
        int dirNum = 0;
        //每次导数据都只选择一定时间段的进行导入导出
        for (File f : filelist) {
            fileNum++;
            if (f.getParentFile().getAbsolutePath().compareTo(startFile) > 0 && f.getParentFile().getAbsolutePath().compareTo(endFile) <= 0) {
                if (!lfs.contains(f.getParentFile())) {
                    lfs.add(f.getParentFile());
                    dirNum++;
                }

            }
        }
        LOGGER.info("文件数：" + fileNum + " 目录数：" + dirNum);


        //遍历目录 2016\\08\\01 格式的目录
        for (final File f : lfs) {
            LOGGER.info("directory" + f.getPath());

            // exe.submit(new Runnable() {
            //    public void run() {

            //十个线程，先拷贝到云桌面

            String pattern = Pattern.quote(System.getProperty("file.separator"));
            String[] file = f.getAbsolutePath().split(pattern);
            String mon = file[3];
            String date = file[4];
            String y = file[2];
            File newDirectory = new File(zhongzhuanDir + File.separator +
                    y + File.separator + mon + File.separator + date);
            if (newDirectory.exists()) {
                newDirectory.delete();
            }
            LOGGER.info("ddirectory" + mon + "-" + y + "-" + date);
            newDirectory.mkdirs();
            LOGGER.info("create dir:" + newDirectory.getAbsolutePath());
            File[] fl = f.listFiles();
            //拷贝目录下的每个文件
            List<File> zips = new ArrayList<File>();
            for (File fs : fl) {
                try {
                    long start = System.currentTimeMillis();
                    // FileUtils.copyFile(fs, new File(newDirectory + File.separator + fs.getName()));
                    // LOGGER.info("拷贝完成："+fs.getAbsolutePath()+" 耗时："+(System.currentTimeMillis()-start));
                    if (fs.getName().endsWith("001") || fs.getName().endsWith("7z")) {
                        //zips.add(new File(newDirectory + File.separator + fs.getName()));
                        zips.add(fs);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

            //遍历文件，并且取001开头的文件,进行解压
            for (File zipf : zips) {
                long start1 = System.currentTimeMillis();
                String newfilePath=newDirectory + File.separator + zipf.getName().substring(0, zipf.getName().length() - 6) + "txt";
                if(zipf.getName().endsWith("001")) {
                    OpenMultipartArchive7z.unzip7z(zipf.getAbsolutePath(), newDirectory.getAbsolutePath());
                }else if(zipf.getName().endsWith("7z")){
                    Open7zAndZip.apache7ZDecomp(zipf.getAbsolutePath(),newDirectory.getAbsolutePath());
                }
               // File ftxt = new File(newfilePath);
                File[] lftxt = newDirectory.listFiles();
                for(File ftxt:lftxt) {
                    LOGGER.info("文件解压完成：" + zipf.getAbsolutePath() + "大小：" + zipf.length() / (1024 * 1024) + "MB,得到新文件:" + ftxt.getAbsolutePath() + " 耗时：" + (System.currentTimeMillis() - start1) / (1000 * 60) + "分钟");
                    //File[] fult = new File(newDirectory + File.separator + zipf.getName()).listFiles();
                    long start2 = System.currentTimeMillis();
                    if (ftxt.getName().contains("index")) {
                        uploadToHdfs(ftxt, indexnewPath + "/" + y + "/" + mon + "/" + date, start2);
                    }
                    if (ftxt.getName().contains("stock_status")) {
                        uploadToHdfs(ftxt, stockstatusnewPath + "/" + y + "/" + mon + "/" + date, start2);

                    }
                    if (ftxt.getName().contains("order")) {
                        uploadToHdfs(ftxt, ordernewPath + "/" + y + "/" + mon + "/" + date, start2);

                    }
                    if (ftxt.getName().contains("trade")) {
                        uploadToHdfs(ftxt, tradenewPath + "/" + y + "/" + mon + "/" + date, start2);

                    }
                    if (ftxt.getName().contains("snap_spot")) {
                        uploadToHdfs(ftxt, snapnewPath + "/" + y + "/" + mon + "/" + date, start2);

                    }
                    if (ftxt.getName().contains("snap_level")) {
                        uploadToHdfs(ftxt, snaplevelnewPath + "/" + y + "/" + mon + "/" + date, start2);
                    }
                    LOGGER.info("上传到hdfs后删除已上传完毕的行情文件:" + ftxt.getAbsolutePath());
                    org.apache.commons.io.FileUtils.deleteQuietly(ftxt);
                }
            }
            LOGGER.info("完成日期："+y + "/" +  mon + "/" + date+"的上传！");
            //   }
            // });
        }


    }

    public static void uploadToHdfs(File ftxt, String path, long start) {
        try {
            //LOGGER.info("path----"+path);
            HdfsUtil.uploadFile(ftxt.getAbsolutePath(), path);
            LOGGER.info("上传文件完成：" + ftxt.getAbsolutePath() + " hdfspath:" + (path + "/" + ftxt.getName()) + " 耗时：" + (System.currentTimeMillis() - start));
        } catch (IOException e) {
            e.printStackTrace();
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
    }


    @Test
    public void testNormal() {
        LOGGER.info("dd" + "2016\\08\\31".compareTo("2017\\09\\31"));
        LOGGER.info("am_hq_snap_spot.7z.001".substring(0, new String("am_hq_snap_spot.7z.001").length() - 6) + "txt");
    }


    @Test
    public void testUnzipFile() throws Exception {
        // apache7ZDecomp("F:\\HIS_SZL2_ALL_Data\\2016\\06\\01\\am_hq_order_spot.7z.001","D:\\tmp\\mddata\\2016\\06\\30");
        // un7z("F:\\HIS_SZL2_ALL_Data\\2016\\06\\01\\am_hq_index.7z.001","D:\\tmp\\mddata\\2016\\06\\30",null);
        //File f = new File("F:\\\\HIS_SZL2_ALL_Data\\\\2016\\\\06\\\\01\\\\am_hq_order_spot.7z.001");
        //LOGGER.info(f.length() * 1.0d / (1024 * 1024 * 1.0d) + "MB");
    }


}
