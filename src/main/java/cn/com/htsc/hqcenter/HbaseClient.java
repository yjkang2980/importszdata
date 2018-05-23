package cn.com.htsc.hqcenter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * @author
 * @version $Id:
 * @Date created in 2017/9/12 19:46
 * @Description
 */
public class HbaseClient {

    private static final Logger LOGGER = LoggerFactory.getLogger(HbaseClient.class);

    public static void main(String[] args) {



    }
    private static Configuration conf;
    private static Connection conn;
    private static Table table;
    static{
        conf = new Configuration();
        //conf.addResource("hbase-site.xml");//指定文件加载

        conf.setInt("hbase.rpc.timeout",20000);
        conf.setInt("zookeeper.session.timeout",10000);
        conf.set("hbase.zookeeper.quorum", "ip-168-61-2-26,ip-168-61-2-27,ip-168-61-2-28");
        conf.set("hbase.htable.threads.max", "1000");
        conf.set("hbase.zookeeper.property.clientPort", "2181");

        conf = HBaseConfiguration.create(conf);

        try {
            conn = ConnectionFactory.createConnection(conf);

        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        try {
            table = conn.getTable(TableName.valueOf("MDC:MDStockRecord"));
        } catch (IOException e1) {
            // TODO Auto-generated catch block
            e1.printStackTrace();
        }

        LOGGER.info("初始化创建hbase成功.....");
    }



    @Test
    public void testHbase() throws IOException {
        Scan scan=new Scan();
        scan.setMaxVersions();
        //指定最多返回的Cell数目。用于防止一行中有过多的数据，导致OutofMemory错误。
        // scan.setBatch(1000);
        scan.setMaxResultSize(19000);

        ResultScanner rs = table.getScanner(scan);

        for (Result r : rs) {
            for (KeyValue kv : r.raw()) {
                System.out.println(
                        Bytes.toString(kv.getRow())+"-"+
                                Bytes.toString(kv.getFamily())+"-"+
                                Bytes.toString(kv.getQualifier())+"-"+
                                Bytes.toString(kv.getValue())+"-"+
                                kv.getTimestamp());
            }
        }

        rs.close();

    }


}
