package cn.com.htsc.hqcenter;

import org.apache.log4j.Logger;
import org.junit.Test;

import java.io.File;
import java.util.*;

/**
 * @author
 * @version $Id:
 * @Date created in 2017/9/15 9:07
 * @Description
 */
public class FileTypeExplorer {
private static final Logger LOGGER = Logger.getLogger(FileTypeExplorer.class);

    private static String filePath = "";
    private static String zhongzhuanDir = "";
    private static String startFile = "";
    private static String endFile = "";
    private static List<File> filelist = new ArrayList<File>();


    //硬盘里面的文件类型:ZIP_OK(需排除)、zip、7z、7z分卷、txt
    private static Set<String> fileType = new HashSet<String>();

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
                      //System.out.println("---" + strFileName);
                    filelist.add(files[i]);
                    if(!strFileName.contains(".")){
                        LOGGER.info(strFileName);
                    }else{
                        int index1 = strFileName.lastIndexOf(".zip");
                        int index2 = strFileName.lastIndexOf(".7z");
                        if(index1>0){
                           if(!fileType.contains(strFileName.substring(index1))){
                               fileType.add(strFileName.substring(index1));
                           }
                        }else if(index2>0){
                            if(!fileType.contains(strFileName.substring(index2))){
                                fileType.add(strFileName.substring(index2));
                            }
                        }else{
                            if(!fileType.contains(strFileName.substring(strFileName.lastIndexOf(".")))){
                                fileType.add(strFileName.substring(strFileName.lastIndexOf(".")));
                            }
                        }
                    }
                } else {
                    continue;
                }
            }

        }
        return filelist;
    }

    @Test
    public void test1(){
        LOGGER.info("index:"+"asdfa.asdfasdf.asdfasf".lastIndexOf(".a"));
    }

    public static void main(String[] args) {
        getFileList(filePath);
        for(String en: fileType){
            LOGGER.info("fileType:"+en);
        }
    }


}
