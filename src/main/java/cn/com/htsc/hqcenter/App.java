package cn.com.htsc.hqcenter;

import org.junit.Test;
import sun.misc.BASE64Encoder;

import javax.crypto.Cipher;
import java.io.*;
import java.security.Key;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

/**
 * Hello world!
 *
 */
public class App 
{

    /**
     *
     * 使用DES加密与解密,可对byte[],String类型进行加密与解密 密文可使用String,byte[]存储.
     *
     * 方法: void getKey(String strKey)从strKey的字条生成一个Key
     *
     * String getEncString(String strMing)对strMing进行加密,返回String密文 String
     * getDesString(String strMi)对strMin进行解密,返回String明文
     *
     *byte[] getEncCode(byte[] byteS)byte[]型的加密 byte[] getDesCode(byte[]
     * byteD)byte[]型的解密
     */
    Key key;

    public static void main( String[] args ) throws IOException
    {
        System.out.println( "Hello World!" );
        FileWriter fw=new FileWriter("test.txt");

        fw.write("test1");
        fw.flush();
        fw.close();
        FileReader fr=new FileReader("test.txt");
        BufferedReader br=new BufferedReader(fr);
       System.out.println(br.readLine());
       br.close();


        Calendar calendar= Calendar.getInstance();
        int year=calendar.get(Calendar.YEAR);
        int month=calendar.get(Calendar.MONTH);
        int day=calendar.get(Calendar.DAY_OF_MONTH);
         final  String a=new String();

        final  String b=new String();
        calendar.set(year,month,day,02,00,00);
        Date date=calendar.getTime();
        Timer timer =new Timer();
        timer.schedule(new TimerTask() {
            @Override
            public void run() {
//System.out.println("a:"+(a.concat("aa"))+"---"+ LocalDateTime.now().format(DateTimeFormatter.BASIC_ISO_DATE));
            }
        },date,2000);

        calendar.set(year,month,day,01,00,00);
        Date date2=calendar.getTime();
        //Timer timer =new Timer();
        timer.schedule(new TimerTask() {
            @Override
            public void run() {
                System.out.println("b:"+(b.concat("bb")));
            }
        },date2,3000);

    }

    @Test
    public void test(){
        TreeMap<Double,Object> t=new TreeMap<>(new Comparator<Double>() {
            @Override
            public int compare(Double o1, Double o2) {
                return o2.compareTo(o1);
            }
        });

        TreeMap<Double,Object> t2=new TreeMap<>();

        t.put(1.0,22);t.put(4.0,99);t.put(3.0,66);t.put(5.0,100);

        t2.put(1.0,22);t2.put(4.0,99);t2.put(3.0,66);
        System.out.println(t.toString()+"-"+t2.toString());

        Map.Entry e=t.ceilingEntry(3.0);
        System.out.println(t.toString()+"-"+e.toString()+"-"+t2.toString());

    }


    @Test
    public void testStringAndChar(){
        System.out.println("S".equals(""+'S'));
    }


    @Test
    public void test1(){
        System.out.println(getEncString("cfets"));
    }
    public String getEncString(String strMing) {
        byte[] byteMi = null;
        byte[] byteMing = null;
        String strMi = "";
        BASE64Encoder base64en = new BASE64Encoder();
        try {
            byteMing = strMing.getBytes("UTF8");
            byteMi = this.getEncCode(byteMing);
            strMi = base64en.encode(byteMi);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            base64en = null;
            byteMing = null;
            byteMi = null;
        }
        return strMi;
    }

    /**
     * 加密以byte[]明文输入,byte[]密文输出
     *
     * @param byteS
     * @return
     */
    private byte[] getEncCode(byte[] byteS) {
        byte[] byteFina = null;
        Cipher cipher;
        try {
            cipher = Cipher.getInstance("DES");
            cipher.init(Cipher.ENCRYPT_MODE, key);
            byteFina = cipher.doFinal(byteS);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            cipher = null;
        }
        return byteFina;
    }
}
