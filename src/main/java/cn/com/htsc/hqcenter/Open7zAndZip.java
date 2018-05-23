package cn.com.htsc.hqcenter;

import net.sf.sevenzipjbinding.*;
import net.sf.sevenzipjbinding.impl.RandomAccessFileInStream;
import net.sf.sevenzipjbinding.simple.ISimpleInArchive;
import net.sf.sevenzipjbinding.simple.ISimpleInArchiveItem;
import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.archivers.sevenz.SevenZArchiveEntry;
import org.apache.commons.compress.archivers.sevenz.SevenZFile;
import org.apache.commons.compress.archivers.zip.ZipArchiveInputStream;
//import org.apache.commons.io.IOUtils;
import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;
import org.junit.Test;

import java.io.*;
import java.util.Arrays;

/**
 * @author
 * @version $Id:
 * @Date created in 2017/9/15 9:08
 * @Description
 */
public class Open7zAndZip {

    private static Logger LOGGER = Logger.getLogger(Open7zAndZip.class);

   /**
     * @param orgPath 源压缩文件地址
     * @param tarpath 解压后存放的目录地址
     * @author kxl
     */
    public static void apache7ZDecomp(String orgPath, String tarpath) {

        try {
            SevenZFile sevenZFile = new SevenZFile(new File(orgPath));
            SevenZArchiveEntry entry = sevenZFile.getNextEntry();
            while (entry != null) {

                // System.out.println(entry.getName());
                if (entry.isDirectory()) {

                    new File(tarpath + entry.getName()).mkdirs();
                    entry = sevenZFile.getNextEntry();
                    continue;
                }
                FileOutputStream out = new FileOutputStream(tarpath
                        + File.separator + entry.getName());
                byte[] content = new byte[(int) entry.getSize()];
                sevenZFile.read(content, 0, content.length);
                out.write(content);
                out.close();
                entry = sevenZFile.getNextEntry();
            }
            sevenZFile.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static int un7z(String file7zPath, final String outPutPath, String passWord) throws Exception {
        IInArchive archive;
        RandomAccessFile randomAccessFile;
        randomAccessFile = new RandomAccessFile(file7zPath, "r");
        archive = SevenZip.openInArchive(null, new RandomAccessFileInStream(randomAccessFile), passWord);
        int numberOfItems = archive.getNumberOfItems();
        ISimpleInArchive simpleInArchive = archive.getSimpleInterface();
        for (final ISimpleInArchiveItem item : simpleInArchive.getArchiveItems()) {
            final int[] hash = new int[]{0};
            if (!item.isFolder()) {
                ExtractOperationResult result;
                final long[] sizeArray = new long[1];
                result = item.extractSlow(new ISequentialOutStream() {
                    public int write(byte[] data) throws SevenZipException {
                        try {
                            IOUtils.write(data, new FileOutputStream(new File(outPutPath + File.separator + item.getPath()), true));
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                        hash[0] ^= Arrays.hashCode(data); // Consume data
                        sizeArray[0] += data.length;
                        return data.length; // Return amount of consumed
                    }
                }, passWord);
                if (result == ExtractOperationResult.OK) {
                    LOGGER.debug(String.format("%9X | %10s | %s", hash[0], sizeArray[0], item.getPath()));
                } else {
                    LOGGER.debug("Error extracting item: " + result);
                }
            }
        }
        archive.close();
        randomAccessFile.close();

        return numberOfItems;
    }


    /**
     * 把zip文件解压到指定的文件夹
     * @param zipFilePath zip文件路径, 如 "D:/test/aa.zip"
     * @param saveFileDir 解压后的文件存放路径, 如"D:/test/" ()
     */
    public static void apacheUnzip(String zipFilePath, String saveFileDir) {
        if(!saveFileDir.endsWith("\\") && !saveFileDir.endsWith("/") ){
            saveFileDir += File.separator;
        }
        File dir = new File(saveFileDir);
        if(!dir.exists()){
            dir.mkdirs();
        }
        File file = new File(zipFilePath);
        if (file.exists()) {
            InputStream is = null;
            ZipArchiveInputStream zais = null;
            try {
                is = new FileInputStream(file);
                zais = new ZipArchiveInputStream(is);
                ArchiveEntry archiveEntry = null;
                while ((archiveEntry = zais.getNextEntry()) != null) {
                    // 获取文件名
                    String entryFileName = archiveEntry.getName();
                    // 构造解压出来的文件存放路径
                    String entryFilePath = saveFileDir + entryFileName;
                    OutputStream os = null;
                    try {
                        // 把解压出来的文件写到指定路径
                        File entryFile = new File(entryFilePath);
                        if(entryFileName.endsWith("/")){
                            entryFile.mkdirs();
                        }else{
                            os = new BufferedOutputStream(new FileOutputStream(
                                    entryFile));
                            byte[] buffer = new byte[1024*1024*10];
                            int len = -1;
                            while((len = zais.read(buffer)) != -1) {
                                os.write(buffer, 0, len);
                            }
                        }
                    } catch (IOException e) {
                        throw new IOException(e);
                    } finally {
                        if (os != null) {
                            os.flush();
                            os.close();
                        }
                    }

                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            } finally {
                try {
                    if (zais != null) {
                        zais.close();
                    }
                    if (is != null) {
                        is.close();
                    }
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }




    @Test
    public void testUnzipFile() throws Exception {
         //apache7ZDecomp("F:\\HIS_SZL2_ALL_Data\\2016\\08\\31\\am_stock_status.7z.001","D:\\tmp\\mddata\\2016\\06\\30");
       //  un7z("F:\\HIS_SZL2_ALL_Data\\2016\\06\\01\\am_hq_index.7z.001","D:\\tmp\\mddata\\2016\\06\\30",null);
        //File f = new File("F:\\\\HIS_SZL2_ALL_Data\\\\2016\\\\06\\\\01\\\\am_hq_order_spot.7z.001");
        //LOGGER.info(f.length() * 1.0d / (1024 * 1024 * 1.0d) + "MB");
        apacheUnzip("F:\\HIS_SZL2_ALL_Data\\2009\\02\\SZL2_SNAPSHOTDW_20090202.zip","D:\\\\tmp\\\\mddata\\\\2016\\\\06\\\\30");

    }


}
