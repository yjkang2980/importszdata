package cn.com.htsc.hqcenter;

/**
 * @author
 * @version $Id:
 * @Date created in 2017/9/14 9:21
 * @Description
 */

import net.sf.sevenzipjbinding.*;
import net.sf.sevenzipjbinding.impl.RandomAccessFileInStream;
import net.sf.sevenzipjbinding.impl.VolumedArchiveInStream;
import org.junit.Test;

import java.io.*;
import java.util.HashMap;
import java.util.Map;

public class OpenMultipartArchive7z {
    /**
     * In this example we use VolumedArchiveInStream class only.
     * It means, we doesn't pass instances of this class directly
     * to 7-Zip, so not complete implementation
     * of {@link IArchiveOpenVolumeCallback} is required.
     * See VolumedArchiveInStream JavaDoc for more information
     */
    private static class ArchiveOpenVolumeCallback
            implements IArchiveOpenVolumeCallback {

        /**
         * Cache for opened file streams
         */
        private Map<String, RandomAccessFile> openedRandomAccessFileList =
                new HashMap<String, RandomAccessFile>();

        /**
         * This method doesn't needed, if using with VolumedArchiveInStream
         * and pass the name of the first archive in constructor.
         * (Use two argument constructor)
         *
         * @see IArchiveOpenVolumeCallback#getProperty(PropID)
         */
        public Object getProperty(PropID propID) throws SevenZipException {
            return null;
        }

        /**
         *
         * The name of the required volume will be calculated out of the
         * name of the first volume and volume index. If you need
         * need volume index (integer) you will have to parse filename
         * and extract index.
         *
         * <pre>
         * int index = filename.substring(filename.length() - 3,
         *         filename.length());
         * </pre>
         *
         */
        public IInStream getStream(String filename) throws SevenZipException {
            try {
                // We use caching of opened streams, so check cache first
                RandomAccessFile randomAccessFile = openedRandomAccessFileList
                        .get(filename);
                if (randomAccessFile != null) { // Cache hit.
                    // Move the file pointer back to the beginning
                    // in order to emulating new stream
                    randomAccessFile.seek(0);
                    return new RandomAccessFileInStream(randomAccessFile);
                }

                // Nothing useful in cache. Open required volume.
                randomAccessFile = new RandomAccessFile(filename, "r");

                // Put new stream in the cache
                openedRandomAccessFileList.put(filename, randomAccessFile);

                return new RandomAccessFileInStream(randomAccessFile);
            } catch (FileNotFoundException fileNotFoundException) {
                // Required volume doesn't exist. This happens if the volume:
                // 1. never exists. 7-Zip doesn't know how many volumes should
                //    exist, so it have to try each volume.
                // 2. should be there, but doesn't. This is an error case.

                // Since normal and error cases are possible,
                // we can't throw an error message
                return null; // We return always null in this case
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        /**
         * Close all opened streams
         */
        void close() throws IOException {
            for (RandomAccessFile file : openedRandomAccessFileList.values()) {
                file.close();
            }
        }
    }


    @Test
    public void tgest1(){
        unzip7z("F:\\HIS_SZL2_ALL_Data\\2016\\08\\31\\am_hq_index.7z.001","D:\\tmp\\mddata\\2016\\06\\30\\am_hq_order_spot.txt");
    }


    public static void unzip7z(String file,String outfile){
        ArchiveOpenVolumeCallback archiveOpenVolumeCallback = null;
        IInArchive inArchive = null;
        //file="F:\\HIS_SZL2_ALL_Data\\2016\\06\\01\\am_hq_order_spot.7z.001";
        //String outfile="D:\\tmp\\mddata\\2016\\06\\30\\aaa.txt";
        try {

            archiveOpenVolumeCallback = new ArchiveOpenVolumeCallback();
            inArchive = SevenZip.openInArchive(ArchiveFormat.SEVEN_ZIP,
                    new VolumedArchiveInStream(file,
                            archiveOpenVolumeCallback));

            System.out.println("   Size   | Compr.Sz. | Filename");
            System.out.println("----------+-----------+---------");
            int itemCount = inArchive.getNumberOfItems();
            for (int i = 0; i < itemCount; i++) {
                System.out.println(String.format("%9s | %9s | %s", //
                        inArchive.getProperty(i, PropID.SIZE),
                        inArchive.getProperty(i, PropID.PACKED_SIZE),
                        inArchive.getProperty(i, PropID.PATH)));
                final BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(new File(outfile+File.separator+inArchive.getProperty(i, PropID.PATH))));
                inArchive.extractSlow(i, new ISequentialOutStream() {
                    public int write(byte[] data) throws SevenZipException {
                        try {
                            bos.write(data);

                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                        return data.length;
                    }
                });
                bos.close();


            }
        } catch (Exception e) {
            System.err.println("Error occurs: " + e);
        } finally {
            if (inArchive != null) {
                try {
                    inArchive.close();
                } catch (SevenZipException e) {
                    System.err.println("Error closing archive: " + e);
                }
            }
            if (archiveOpenVolumeCallback != null) {
                try {
                    archiveOpenVolumeCallback.close();
                } catch (IOException e) {
                    System.err.println("Error closing file: " + e);
                }
            }
        }
        System.out.println("exists:"+new File(outfile).exists());
    }



    public static void main(String[] args) {
        //if (args.length == 0) {
        //    System.out.println(
       //             "Usage: java OpenMultipartArchive7z <first-volume>");
       ////     return;
     //   }
        ArchiveOpenVolumeCallback archiveOpenVolumeCallback = null;
        IInArchive inArchive = null;
        String file="F:\\HIS_SZL2_ALL_Data\\2016\\06\\01\\am_hq_order_spot.7z.001";
        String outfile="D:\\tmp\\mddata\\2016\\06\\30\\aaa.txt";
        try {

            archiveOpenVolumeCallback = new ArchiveOpenVolumeCallback();
            inArchive = SevenZip.openInArchive(ArchiveFormat.SEVEN_ZIP,
                    new VolumedArchiveInStream(file,
                            archiveOpenVolumeCallback));

            System.out.println("   Size   | Compr.Sz. | Filename");
            System.out.println("----------+-----------+---------");
            int itemCount = inArchive.getNumberOfItems();
            for (int i = 0; i < itemCount; i++) {
                System.out.println(String.format("%9s | %9s | %s", //
                        inArchive.getProperty(i, PropID.SIZE),
                        inArchive.getProperty(i, PropID.PACKED_SIZE),
                        inArchive.getProperty(i, PropID.PATH)));
               final BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(new File(outfile)));
                     inArchive.extractSlow(i, new ISequentialOutStream() {
                         public int write(byte[] data) throws SevenZipException {
                             try {
                                 bos.write(data);

                             } catch (IOException e) {
                                 e.printStackTrace();
                             }
                             return data.length;
                         }
                     });
                     bos.close();


            }
        } catch (Exception e) {
            System.err.println("Error occurs: " + e);
        } finally {
            if (inArchive != null) {
                try {
                    inArchive.close();
                } catch (SevenZipException e) {
                    System.err.println("Error closing archive: " + e);
                }
            }
            if (archiveOpenVolumeCallback != null) {
                try {
                    archiveOpenVolumeCallback.close();
                } catch (IOException e) {
                    System.err.println("Error closing file: " + e);
                }
            }
        }
    }
}