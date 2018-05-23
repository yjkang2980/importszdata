package cn.com.htsc.hqcenter.dataretrive;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @author 010571
 * @version $Id:
 * @Date created in 2018/4/18 16:26
 * @Description
 */
public class HBaseUtil {

    static SimpleDateFormat format = new SimpleDateFormat("yyyyMMdd");
    public static Connection  getConn(){
        Configuration  conf = new Configuration();
        //conf.addResource("hbase-site.xml");//指定文件加载

        conf.setInt("hbase.rpc.timeout",20000);
        conf.setInt("zookeeper.session.timeout",10000);
        conf.set("hbase.zookeeper.quorum", "arch-bd-zookeeper2,arch-bd-zookeeper4,arch-bd-zookeeper1,arch-bd-zookeeper3,arch-bd-zookeeper5");
        // conf.set("hbase.htable.threads.max", "1000");
        conf.set("hbase.zookeeper.property.clientPort", "2181");

        conf = HBaseConfiguration.create(conf);
        try {
            Connection conn = ConnectionFactory.createConnection(conf);

            return conn;
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return null;
        // LOGGER.info("初始化创建hbase成功.....");
    }


    public static boolean writeStatus(String status) throws Exception{
        Table table = getConn().getTable(TableName.valueOf("MDC:MDAppStateRecord"));
        Put p =new Put(Bytes.toBytes(new String((java.lang.Long.MAX_VALUE - new Date().getTime()) + "SZLV2_DATARETRIEVE")));
        p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("date"),Bytes.toBytes(format.format(new Date())));
        p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("status"),Bytes.toBytes(status));
        //p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("TotalVolumeTrade"),newTVolume);
        //lps.add(p);
        table.put(p);
        return true;
    }


}
