package cn.com.htsc.hqcenter;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.Charset;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.MD5Hash;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.csvreader.CsvWriter;


/**
 * @author
 * @version $Id:
 * @Date created in 2018/1/3 15:32
 * @Description
 */
public class HbaseDataExame {
    private static final Logger LOGGER = LoggerFactory.getLogger(HbaseDataExame.class);
    private static Configuration conf;
    private static Connection conn;
    private static Table table;

    private static String[] cols = new String[] { "Buy1OrderQty", "Buy1Price", "Buy1NumOrders", "Buy1NoOrders",
            "Buy2OrderQty", "Buy2Price", "Buy2NumOrders", "Buy2NoOrders", "Buy3OrderQty", "Buy3Price", "Buy3NumOrders",
            "Buy3NoOrders", "Buy4OrderQty", "Buy4Price", "Buy4NumOrders", "Buy4NoOrders", "Buy5OrderQty", "Buy5Price",
            "Buy5NumOrders", "Buy5NoOrders", "Buy6OrderQty", "Buy6Price", "Buy6NumOrders", "Buy6NoOrders",
            "Buy7OrderQty", "Buy7Price", "Buy7NumOrders", "Buy7NoOrders", "Buy8OrderQty", "Buy8Price", "Buy8NumOrders",
            "Buy8NoOrders", "Buy8OrderQty", "Buy8Price", "Buy8NumOrders", "Buy8NoOrders", "Buy10OrderQty", "Buy10Price",
            "Buy10NumOrders", "Buy10NoOrders", "Sell1OrderQty", "Sell1Price", "Sell1NumOrders", "Sell1NoOrders",
            "Sell2OrderQty", "Sell2Price", "Sell2NumOrders", "Sell2NoOrders", "Sell3OrderQty", "Sell3Price",
            "Sell3NumOrders", "Sell3NoOrders", "Sell4OrderQty", "Sell4Price", "Sell4NumOrders", "Sell4NoOrders",
            "Sell5OrderQty", "Sell5Price", "Sell5NumOrders", "Sell5NoOrders", "Sell6OrderQty", "Sell6Price",
            "Sell6NumOrders", "Sell6NoOrders", "Sell7OrderQty", "Sell7Price", "Sell7NumOrders", "Sell8NoOrders",
            "Sell8OrderQty", "Sell8Price", "Sell8NumOrders", "Sell8NoOrders", "Sell9OrderQty", "Sell9Price",
            "Sell9NumOrders", "Sell9NoOrders", "Sell10OrderQty", "Sell10Price", "Sell10NumOrders", "Sell10NoOrders",
            "PreClosePx", "NumTrades", "TotalVolumeTrade", "TotalValueTrade", "LastPx", "OpenPx", "ClosePx", "HighPx",
            "LowPx", "DiffPx1", "DiffPx2", "MinPx", "MaxPx", "TotalBidQty", "TotalOfferQty", "WeightedAvgBidPx",
            "WeightedAvgOfferPx", "WithdrawBuyNumber", "WithdrawBuyAmount", "WithdrawBuyMoney", "WithdrawSellNumber",
            "WithdrawSellAmount", "WithdrawSellMoney", "TotalBidNumber", "TotalOfferNumber", "BidTradeMaxDuration",
            "OfferTradeMaxDuration", "NumBidOrders", "NumOfferOrders", "SLYOne", "SLYTwo" };

    private static List<String> colss = Arrays.asList(cols);

    // private static ExecutorService dateExe =
    // Executors.newFixedThreadPool(10);
    private static ExecutorService prdExe = Executors.newFixedThreadPool(300);

    private static BlockingQueue<String> highLowBQ = new ArrayBlockingQueue<String>(10000);
    private static BlockingQueue<String> nullRecBQ = new ArrayBlockingQueue<String>(10000);

    private static BlockingQueue<String> minMaxBQ = new ArrayBlockingQueue<String>(10000);
    private static BlockingQueue<String> volumeBQ = new ArrayBlockingQueue<String>(10000);

    private static BlockingQueue<String> valueBQ = new ArrayBlockingQueue<String>(10000);
    private static BlockingQueue<String> highBQ = new ArrayBlockingQueue<String>(10000);

    private static BlockingQueue<String> lowBQ = new ArrayBlockingQueue<String>(10000);


    private static CsvWriter highLowWriter = new CsvWriter("highLowrecords.csv", ',', Charset.forName("UTF-8"));
    private static CsvWriter minMaxWriter = new CsvWriter("minMaxrecords.csv", ',', Charset.forName("UTF-8"));
    private static CsvWriter volumeWriter = new CsvWriter("volumerecords.csv", ',', Charset.forName("UTF-8"));
    private static CsvWriter valueWriter = new CsvWriter("valuerecords.csv", ',', Charset.forName("UTF-8"));
    private static CsvWriter highWriter = new CsvWriter("highrecords.csv", ',', Charset.forName("UTF-8"));
    private static CsvWriter lowWriter = new CsvWriter("lowrecords.csv", ',', Charset.forName("UTF-8"));
    private static CsvWriter nullWriter = new CsvWriter("nullrecordst.csv", ',', Charset.forName("UTF-8"));

    private static List<String> prdids = new ArrayList<String>();

    static {
        conf = new Configuration();
        // conf.addResource("hbase-site.xml");//指定文件加载

        conf.setInt("hbase.rpc.timeout", 20000);
        conf.setInt("zookeeper.session.timeout", 10000);
        conf.set("hbase.zookeeper.quorum",
                "168.9.65.48,168.9.65.52,168.9.65.53,168.9.65.54,168.9.65.55");
        // conf.set("hbase.htable.threads.max", "1000");
        conf.set("hbase.zookeeper.property.clientPort", "2181");

        conf = HBaseConfiguration.create(conf);

        try {


            conn = ConnectionFactory.createConnection(conf, Executors.newFixedThreadPool(200));

        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        BufferedReader br2;
        try {
            br2 = new BufferedReader(new FileReader("resources/prdInfo.txt"));
            String line2 = null;

            while ((line2 = br2.readLine()) != null) {
                String prdid = line2.split(",")[0];
                String type = line2.split(",")[1];
                if (!type.equals("2")) {
                    continue;
                }
                prdids.add(prdid);
            }
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        LOGGER.info("初始化创建hbase成功.....");
    }

    @Test
    public static void nonTradingDays() throws Exception {
        BufferedReader br = new BufferedReader(new FileReader("resources/nonTradingDay.txt"));
        String line = null;
        CsvWriter csvWriter = new CsvWriter("notradingDaysrecords.csv", ',', Charset.forName("UTF-8"));
        while ((line = br.readLine()) != null) {
            String date = line.split(",")[0];

            BufferedReader br2 = new BufferedReader(new FileReader("resources/prdInfo.txt"));
            String line2 = null;

            while ((line2 = br2.readLine()) != null) {
                String prdid = line2.split(",")[0];
                String type = line2.split(",")[1];
                if (!type.equals("2")) {
                    continue;
                }
                System.out.println("非交易日校验开始：" + prdid + "--" + date);
                Scan scan = new Scan();
                scan.setMaxVersions();
                // 指定最多返回的Cell数目。用于防止一行中有过多的数据，导致OutofMemory错误。
                // scan.setBatch(1000);
                scan.setMaxResultSize(19000);
                String secid = prdid;
                // String targetSecid="395004.SZ";
                SimpleDateFormat format = new SimpleDateFormat("yyyyMMddHHmmssSSS");
                long end = (Long.MAX_VALUE - format.parse(date + "080000000").getTime());
                long start = Long.MAX_VALUE - format.parse(date + "170000000").getTime();
                String startrowKey = MD5Hash.getMD5AsHex(secid.getBytes()).substring(0, 6) + secid + start;
                String endKey = MD5Hash.getMD5AsHex(secid.getBytes()).substring(0, 6) + secid + end;

                // String
                // targetIdStart=MD5Hash.getMD5AsHex(targetSecid.getBytes()).substring(0,6)+targetSecid;
                // LOGGER.info(secid + "-" + startrowKey + "--" + endKey + "-");
                scan.setStartRow(Bytes.toBytes(startrowKey));
                scan.setStopRow(Bytes.toBytes(endKey));
                ResultScanner rs = table.getScanner(scan);
                // List<Put> lps = new ArrayList<>();
                if (null != rs) {

                    for (Result r : rs) {

                        if (null != r) {
                            String row = new String(r.getRow());
                            String times = row.substring(15);
                            if (secid.endsWith("HK")) {
                                times = row.substring(14);
                            }
                            // LOGGER.info(secid + "-" + row + "--" + times +
                            // "-dd");
                            String mdtime = format.format(new Date(Long.MAX_VALUE - Long.parseLong(times)));
                            String[] recs = { secid, row, mdtime };
                            LOGGER.info("非交易日记录：" + Arrays.asList(recs).toString());
                            csvWriter.writeRecord(recs);
                            csvWriter.flush();
                        }
                    }

                }

                System.out.println("非交易日校验结束：" + prdid + "--" + date);
            }
            br2.close();
        }
        csvWriter.close();
        br.close();

    }

    public static void main(String[] args) {
        System.out
                .println("开始梳理数据，起始空字段量:" + cols.length + " list size:" + colss.size() + " prd size:" + prdids.size()+" 第一个参数是开始时间，"
                        + "第二个参数是结束日期");
        final String[] args1=args;
        Runnable r1 = new Runnable() {
            /**
             * @see java.lang.Runnable#run()
             */
            @Override
            public void run() {
                // TODO Auto-generated method stub
                try {
                    getIllegalData(args1);
                } catch (Exception e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }
        };


        ExecutorService exe = Executors.newFixedThreadPool(2);
        exe.submit(r1);
        // exe.submit(r2);

        try {
            writeRec();
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    @Test
    public static void getIllegalData(String[] args) throws Exception {
        final StringBuffer startDate = new StringBuffer(args[0]);
        String endDate = args[1];
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
        Calendar c = Calendar.getInstance();
        c.setTime(sdf.parse(startDate.toString()));

        while (startDate.toString().compareTo(endDate) < 0) {


            try {
                c.add(Calendar.DAY_OF_MONTH, 1);
                Date newDate = c.getTime();
                startDate.delete(0, 8);
                startDate.append(sdf.format(newDate));

                if (c.get(Calendar.DAY_OF_WEEK) == 1 || c.get(Calendar.DAY_OF_WEEK) == 7) {
                    continue;
                }
                //System.out.println("准备开始提交日期:" + startDate.toString());
                recordDirtyData(startDate);
            } catch (Exception e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }


        }

    }

    private static void recordDirtyData(StringBuffer date1) throws Exception {
        final String date = date1.toString();


        for (int i = 0; i < prdids.size(); i++) {
            // String prdid=prdids.get(i);
            final int j = i;
            prdExe.submit(new Runnable() {
                /**
                 * @see java.lang.Runnable#run()
                 */
                @Override
                public void run() {
                    // String prdid =;

                    // TODO Auto-generated method stub
                    try {
                        System.out.println(Thread.currentThread().getName() + "---开始校验:--" + " 日期：" + date);
                        dealWithData(prdids.get(j), date);
                    } catch (Exception e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                    }
                }
            });
        }

    }

    public static void dealWithData(String prdid, String date) throws Exception {

        long lastVolume = 0;
        long lastValue = 0;
        double lasthighPx = 0.0;
        double lastlowPx = 0.0;
        String lastRowKey = "";
        String lastTime = "";
        try {
            table = conn.getTable(TableName.valueOf("MDC:MDStockRecord"));
        } catch (IOException e1) {
            // TODO Auto-generated catch block
            e1.printStackTrace();
        }
        Scan scan = new Scan();
        scan.setMaxVersions();
        // 指定最多返回的Cell数目。用于防止一行中有过多的数据，导致OutofMemory错误。
        // scan.setBatch(1000);
        scan.setMaxResultSize(19000);
        String secid = prdid;
        System.out.println("开始校验:" + secid + " 日期：" + date);
        // String targetSecid="395004.SZ";
        SimpleDateFormat format = new SimpleDateFormat("yyyyMMddHHmmssSSS");
        long end = (Long.MAX_VALUE - format.parse(date + "080000000").getTime());
        long start = Long.MAX_VALUE - format.parse(date + "170000000").getTime();
        String startrowKey = MD5Hash.getMD5AsHex(secid.getBytes()).substring(0, 6) + secid + start;
        String endKey = MD5Hash.getMD5AsHex(secid.getBytes()).substring(0, 6) + secid + end;

        // String
        // targetIdStart=MD5Hash.getMD5AsHex(targetSecid.getBytes()).substring(0,6)+targetSecid;
        // LOGGER.info(secid + "-" + startrowKey + "--" + endKey + "-");
        scan.setStartRow(Bytes.toBytes(startrowKey));
        scan.setStopRow(Bytes.toBytes(endKey));
        ResultScanner rs = table.getScanner(scan);
        // List<Put> lps = new ArrayList<>();
        if (null != rs) {

            for (Result r : rs) {

                if (null != r) {
                    String row = new String(r.getRow());
                    String times = row.substring(15);
                    if (secid.endsWith("HK")) {
                        times = row.substring(14);
                    }
                    // LOGGER.info(secid + "-" + row + "--" + times + "-dd");
                    String mdtime = format.format(new Date(Long.MAX_VALUE - Long.parseLong(times)));
                    List<Cell> ceList = r.listCells();
                    for (Cell cell : ceList) {
                        // String row = Bytes.toString(cell.getRowArray(),
                        // cell.getRowOffset(),
                        // cell.getRowLength());
                        String value = Bytes.toString(cell.getValueArray(), cell.getValueOffset());

                        // 空值列必须指定列名,mdstream这样的列很多为空
                        if (StringUtils.isBlank(value) || value.equals("NaN")) {
                            String quali = Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(),
                                    cell.getQualifierLength());
                            if (colss.contains(quali)) {
                                String[] recs = { secid, row, mdtime, quali, value };
                                //LOGGER.info("空值：" + Arrays.asList(recs).toString());
                                nullRecBQ.put(Arrays.asList(recs).toString());
                                // nullWriter.writeRecord(recs);
                                // nullWriter.flush();
                            }
                        }
                    }

                    byte[] buy1p = r.getValue(Bytes.toBytes("Detail"), Bytes.toBytes("Buy1Price"));
                    byte[] buy1qtya = r.getValue(Bytes.toBytes("Detail"), Bytes.toBytes("Buy1OrderQty"));

                    byte[] buy2p = r.getValue(Bytes.toBytes("Detail"), Bytes.toBytes("Buy2Price"));
                    byte[] buy2qtya = r.getValue(Bytes.toBytes("Detail"), Bytes.toBytes("Buy2OrderQty"));

                    byte[] buy3p = r.getValue(Bytes.toBytes("Detail"), Bytes.toBytes("Buy3Price"));
                    byte[] buy3qtya = r.getValue(Bytes.toBytes("Detail"), Bytes.toBytes("Buy3OrderQty"));

                    byte[] buy4p = r.getValue(Bytes.toBytes("Detail"), Bytes.toBytes("Buy4Price"));
                    byte[] buy4qtya = r.getValue(Bytes.toBytes("Detail"), Bytes.toBytes("Buy4OrderQty"));

                    byte[] buy5p = r.getValue(Bytes.toBytes("Detail"), Bytes.toBytes("Buy5Price"));
                    byte[] buy5qtya = r.getValue(Bytes.toBytes("Detail"), Bytes.toBytes("Buy5OrderQty"));

                    byte[] buy6p = r.getValue(Bytes.toBytes("Detail"), Bytes.toBytes("Buy6Price"));
                    byte[] buy6qtya = r.getValue(Bytes.toBytes("Detail"), Bytes.toBytes("Buy6OrderQty"));

                    byte[] buy7p = r.getValue(Bytes.toBytes("Detail"), Bytes.toBytes("Buy7Price"));
                    byte[] buy7qtya = r.getValue(Bytes.toBytes("Detail"), Bytes.toBytes("Buy7OrderQty"));

                    byte[] buy8p = r.getValue(Bytes.toBytes("Detail"), Bytes.toBytes("Buy8Price"));
                    byte[] buy8qtya = r.getValue(Bytes.toBytes("Detail"), Bytes.toBytes("Buy8OrderQty"));

                    byte[] buy9p = r.getValue(Bytes.toBytes("Detail"), Bytes.toBytes("Buy9Price"));
                    byte[] buy9qtya = r.getValue(Bytes.toBytes("Detail"), Bytes.toBytes("Buy9OrderQty"));

                    byte[] buy10p = r.getValue(Bytes.toBytes("Detail"), Bytes.toBytes("Buy10Price"));
                    byte[] buy10qtya = r.getValue(Bytes.toBytes("Detail"), Bytes.toBytes("Buy10OrderQty"));

                    byte[] sell1p = r.getValue(Bytes.toBytes("Detail"), Bytes.toBytes("Sell1Price"));
                    byte[] sell1qtya = r.getValue(Bytes.toBytes("Detail"), Bytes.toBytes("Sell1OrderQty"));

                    byte[] sell2p = r.getValue(Bytes.toBytes("Detail"), Bytes.toBytes("Sell2Price"));
                    byte[] sell2qtya = r.getValue(Bytes.toBytes("Detail"), Bytes.toBytes("Sell2OrderQty"));

                    byte[] sell3p = r.getValue(Bytes.toBytes("Detail"), Bytes.toBytes("Sell3Price"));
                    byte[] sell3qtya = r.getValue(Bytes.toBytes("Detail"), Bytes.toBytes("Sell3OrderQty"));

                    byte[] sell4p = r.getValue(Bytes.toBytes("Detail"), Bytes.toBytes("Sell4Price"));
                    byte[] sell4qtya = r.getValue(Bytes.toBytes("Detail"), Bytes.toBytes("Sell4OrderQty"));

                    byte[] sell5p = r.getValue(Bytes.toBytes("Detail"), Bytes.toBytes("Sell5Price"));
                    byte[] sell5qtya = r.getValue(Bytes.toBytes("Detail"), Bytes.toBytes("Sell5OrderQty"));

                    byte[] sell6p = r.getValue(Bytes.toBytes("Detail"), Bytes.toBytes("Sell6Price"));
                    byte[] sell6qtya = r.getValue(Bytes.toBytes("Detail"), Bytes.toBytes("Sell6OrderQty"));

                    byte[] sell7p = r.getValue(Bytes.toBytes("Detail"), Bytes.toBytes("Sell7Price"));
                    byte[] sell7qtya = r.getValue(Bytes.toBytes("Detail"), Bytes.toBytes("Sell7OrderQty"));

                    byte[] sell8p = r.getValue(Bytes.toBytes("Detail"), Bytes.toBytes("Sell8Price"));
                    byte[] sell8qtya = r.getValue(Bytes.toBytes("Detail"), Bytes.toBytes("Sell8OrderQty"));

                    byte[] sell9p = r.getValue(Bytes.toBytes("Detail"), Bytes.toBytes("Sell9Price"));
                    byte[] sell9qtya = r.getValue(Bytes.toBytes("Detail"), Bytes.toBytes("Sell9OrderQty"));

                    byte[] sell10p = r.getValue(Bytes.toBytes("Detail"), Bytes.toBytes("Sell10Price"));
                    byte[] sell10qtya = r.getValue(Bytes.toBytes("Detail"), Bytes.toBytes("Sell10OrderQty"));

                    if (buy1p != null && buy1qtya != null){
                        String buy1px = new String(buy1p);
                        double buy1pxd=Double.parseDouble(buy1px);
                        String buy1qty = new String(buy1qtya);
                        if(buy1pxd==0.0 && buy1qty.substring(0, 1).compareTo("0") > 0){
                            String[] recs = { secid, row, mdtime, buy1px, buy1qty, "买1成交价格为0，成交量不为0" };
                            //LOGGER.info("有成交价格为0，交易量不为0的记录：" + Arrays.asList(recs).toString());
                            // nullWriter.writeRecord(recs);
                            // nullWriter.flush();
                            nullRecBQ.put(Arrays.asList(recs).toString());
                        }
                    }

                    if ( buy2p != null && buy2qtya != null){
                        String buy2px = new String(buy2p);
                        String buy2qty = new String(buy2qtya);
                        double buy2pxd=Double.parseDouble(buy2px);
                        if(buy2pxd==0.0 && buy2qty.substring(0, 1).compareTo("0") > 0){
                            String[] recs = { secid, row, mdtime, buy2px, buy2qty, "买2成交价格为0，成交量不为0" };
                            //LOGGER.info("有成交价格为0，交易量不为0的记录：" + Arrays.asList(recs).toString());
                            // nullWriter.writeRecord(recs);
                            // nullWriter.flush();
                            nullRecBQ.put(Arrays.asList(recs).toString());
                        }
                    }

                    if (buy3p != null && buy3qtya != null){
                        String buy3px = new String(buy3p);
                        String buy3qty = new String(buy3qtya);
                        double buy3pxd=Double.parseDouble(buy3px);
                        if(buy3pxd==0.0 && buy3qty.substring(0, 1).compareTo("0") > 0){
                            String[] recs = { secid, row, mdtime, buy3px, buy3qty, "买3成交价格为0，成交量不为0" };
                            //LOGGER.info("有成交价格为0，交易量不为0的记录：" + Arrays.asList(recs).toString());
                            // nullWriter.writeRecord(recs);
                            // nullWriter.flush();
                            nullRecBQ.put(Arrays.asList(recs).toString());
                        }
                    }

                    if (buy4p != null && buy4qtya != null){
                        String buy4px = new String(buy4p);
                        String buy4qty = new String(buy4qtya);
                        double buy4pxd=Double.parseDouble(buy4px);
                        if(buy4pxd==0.0 && buy4qty.substring(0, 1).compareTo("0") > 0){
                            String[] recs = { secid, row, mdtime, buy4px, buy4qty, "买4成交价格为0，成交量不为0" };
                            //LOGGER.info("有成交价格为0，交易量不为0的记录：" + Arrays.asList(recs).toString());
                            // nullWriter.writeRecord(recs);
                            // nullWriter.flush();
                            nullRecBQ.put(Arrays.asList(recs).toString());
                        }
                    }

                    if (buy5p != null && buy5qtya != null){
                        String buy5px = new String(buy5p);
                        String buy5qty = new String(buy5qtya);
                        double buy5pxd=Double.parseDouble(buy5px);
                        if(buy5pxd==0.0 && buy5qty.substring(0, 1).compareTo("0") > 0){
                            String[] recs = { secid, row, mdtime, buy5px, buy5qty, "买5成交价格为0，成交量不为0" };
                            //LOGGER.info("有成交价格为0，交易量不为0的记录：" + Arrays.asList(recs).toString());
                            // nullWriter.writeRecord(recs);
                            // nullWriter.flush();
                            nullRecBQ.put(Arrays.asList(recs).toString());
                        }
                    }

                    if (buy6p != null && buy6qtya != null){
                        String buy6px = new String(buy6p);
                        String buy6qty = new String(buy6qtya);
                        double buy6pxd=Double.parseDouble(buy6px);
                        if(buy6pxd==0.0 && buy6qty.substring(0, 1).compareTo("0") > 0){
                            String[] recs = { secid, row, mdtime, buy6px, buy6qty, "买6成交价格为0，成交量不为0" };
                            //LOGGER.info("有成交价格为0，交易量不为0的记录：" + Arrays.asList(recs).toString());
                            // nullWriter.writeRecord(recs);
                            // nullWriter.flush();
                            nullRecBQ.put(Arrays.asList(recs).toString());
                        }
                    }

                    if (buy7p != null && buy7qtya != null){
                        String buy7px = new String(buy7p);
                        String buy7qty = new String(buy7qtya);
                        double buy7pxd=Double.parseDouble(buy7px);
                        if(buy7pxd==0.0 && buy7qty.substring(0, 1).compareTo("0") > 0){
                            String[] recs = { secid, row, mdtime, buy7px, buy7qty, "买7成交价格为0，成交量不为0" };
                            //LOGGER.info("有成交价格为0，交易量不为0的记录：" + Arrays.asList(recs).toString());
                            // nullWriter.writeRecord(recs);
                            // nullWriter.flush();
                            nullRecBQ.put(Arrays.asList(recs).toString());
                        }
                    }

                    if (buy8p != null && buy8qtya != null){
                        String buy8px = new String(buy8p);
                        String buy8qty = new String(buy8qtya);
                        double buy8pxd=Double.parseDouble(buy8px);
                        if(buy8pxd==0.0 && buy8qty.substring(0, 1).compareTo("0") > 0){
                            String[] recs = { secid, row, mdtime, buy8px, buy8qty, "买8成交价格为0，成交量不为0" };
                            //LOGGER.info("有成交价格为0，交易量不为0的记录：" + Arrays.asList(recs).toString());
                            // nullWriter.writeRecord(recs);
                            // nullWriter.flush();
                            nullRecBQ.put(Arrays.asList(recs).toString());
                        }
                    }

                    if (buy9p != null && buy9qtya != null){
                        String buy9px = new String(buy9p);
                        String buy9qty = new String(buy9qtya);
                        double buy9pxd=Double.parseDouble(buy9px);
                        if(buy9pxd==0.0 && buy9qty.substring(0, 1).compareTo("0") > 0){
                            String[] recs = { secid, row, mdtime, buy9px, buy9qty, "买9成交价格为0，成交量不为0" };
                            //LOGGER.info("有成交价格为0，交易量不为0的记录：" + Arrays.asList(recs).toString());
                            // nullWriter.writeRecord(recs);
                            // nullWriter.flush();
                            nullRecBQ.put(Arrays.asList(recs).toString());
                        }
                    }

                    if (buy10p != null && buy10qtya != null){
                        String buy10px = new String(buy10p);
                        String buy10qty = new String(buy10qtya);
                        double buy10pxd=Double.parseDouble(buy10px);
                        if(buy10pxd==0.0 && buy10qty.substring(0, 1).compareTo("0") > 0){
                            String[] recs = { secid, row, mdtime, buy10px, buy10qty, "买10成交价格为0，成交量不为0" };
                            //LOGGER.info("有成交价格为0，交易量不为0的记录：" + Arrays.asList(recs).toString());
                            // nullWriter.writeRecord(recs);
                            // nullWriter.flush();
                            nullRecBQ.put(Arrays.asList(recs).toString());
                        }
                    }

                    if (sell1p != null && sell1qtya != null){
                        String sell1px = new String(sell1p);
                        String sell1qty = new String(sell1qtya);
                        double sell1pxd=Double.parseDouble(sell1px);
                        if(sell1pxd==0.0 && sell1qty.substring(0, 1).compareTo("0") > 0){
                            String[] recs = { secid, row, mdtime, sell1px, sell1qty, "卖1成交价格为0，成交量不为0" };
                            //LOGGER.info("有成交价格为0，交易量不为0的记录：" + Arrays.asList(recs).toString());
                            // nullWriter.writeRecord(recs);
                            // nullWriter.flush();
                            nullRecBQ.put(Arrays.asList(recs).toString());
                        }
                    }

                    if (sell2p != null && sell2qtya != null){
                        String sell2px = new String(sell2p);
                        String sell2qty = new String(sell2qtya);
                        double sell2pxd=Double.parseDouble(sell2px);
                        if(sell2pxd==0.0 && sell2qty.substring(0, 1).compareTo("0") > 0){
                            String[] recs = { secid, row, mdtime, sell2px, sell2qty, "卖2成交价格为0，成交量不为0" };
                            //LOGGER.info("有成交价格为0，交易量不为0的记录：" + Arrays.asList(recs).toString());
                            // nullWriter.writeRecord(recs);
                            // nullWriter.flush();
                            nullRecBQ.put(Arrays.asList(recs).toString());
                        }
                    }

                    if (sell3p != null && sell3qtya != null){
                        String sell3px = new String(sell3p);
                        String sell3qty = new String(sell3qtya);
                        double sell3pxd=Double.parseDouble(sell3px);
                        if(sell3pxd==0.0 && sell3qty.substring(0, 1).compareTo("0") > 0){
                            String[] recs = { secid, row, mdtime, sell3px, sell3qty, "卖3成交价格为0，成交量不为0" };
                            //LOGGER.info("有成交价格为0，交易量不为0的记录：" + Arrays.asList(recs).toString());
                            // nullWriter.writeRecord(recs);
                            // nullWriter.flush();
                            nullRecBQ.put(Arrays.asList(recs).toString());
                        }
                    }

                    if (sell4p != null && sell4qtya != null){
                        String sell4px = new String(sell4p);
                        String sell4qty = new String(sell4qtya);
                        double sell4pxd=Double.parseDouble(sell4px);
                        if(sell4pxd==0.0 && sell4qty.substring(0, 1).compareTo("0") > 0){
                            String[] recs = { secid, row, mdtime, sell4px, sell4qty, "卖4成交价格为0，成交量不为0" };
                            //LOGGER.info("有成交价格为0，交易量不为0的记录：" + Arrays.asList(recs).toString());
                            // nullWriter.writeRecord(recs);
                            // nullWriter.flush();
                            nullRecBQ.put(Arrays.asList(recs).toString());
                        }
                    }

                    if (sell5p != null && sell5qtya != null){
                        String sell5px = new String(sell5p);
                        String sell5qty = new String(sell5qtya);
                        double sell5pxd=Double.parseDouble(sell5px);
                        if(sell5pxd==0.0 && sell5qty.substring(0, 1).compareTo("0") > 0){
                            String[] recs = { secid, row, mdtime, sell5px, sell5qty, "卖5成交价格为0，成交量不为0" };
                            //LOGGER.info("有成交价格为0，交易量不为0的记录：" + Arrays.asList(recs).toString());
                            // nullWriter.writeRecord(recs);
                            // nullWriter.flush();
                            nullRecBQ.put(Arrays.asList(recs).toString());
                        }
                    }

                    if (sell6p != null && sell6qtya != null){
                        String sell6px = new String(sell6p);
                        String sell6qty = new String(sell6qtya);
                        double sell6pxd=Double.parseDouble(sell6px);
                        if(sell6pxd==0.0 && sell6qty.substring(0, 1).compareTo("0") > 0){
                            String[] recs = { secid, row, mdtime, sell6px, sell6qty, "卖6成交价格为0，成交量不为0" };
                            //LOGGER.info("有成交价格为0，交易量不为0的记录：" + Arrays.asList(recs).toString());
                            // nullWriter.writeRecord(recs);
                            // nullWriter.flush();
                            nullRecBQ.put(Arrays.asList(recs).toString());
                        }
                    }

                    if (sell7p != null && sell7qtya != null){
                        String sell7px = new String(sell7p);
                        String sell7qty = new String(sell7qtya);
                        double sell7pxd=Double.parseDouble(sell7px);
                        if(sell7pxd==0.0 && sell7qty.substring(0, 1).compareTo("0") > 0){
                            String[] recs = { secid, row, mdtime, sell7px, sell7qty, "卖7成交价格为0，成交量不为0" };
                            //LOGGER.info("有成交价格为0，交易量不为0的记录：" + Arrays.asList(recs).toString());
                            // nullWriter.writeRecord(recs);
                            // nullWriter.flush();
                            nullRecBQ.put(Arrays.asList(recs).toString());
                        }
                    }

                    if (sell8p != null && sell8qtya != null){
                        String sell8px = new String(sell8p);
                        String sell8qty = new String(sell8qtya);
                        double sell8pxd=Double.parseDouble(sell8px);
                        if(sell8pxd==0.0 && sell8qty.substring(0, 1).compareTo("0") > 0){
                            String[] recs = { secid, row, mdtime, sell8px, sell8qty, "卖8成交价格为0，成交量不为0" };
                            //LOGGER.info("有成交价格为0，交易量不为0的记录：" + Arrays.asList(recs).toString());
                            // nullWriter.writeRecord(recs);
                            // nullWriter.flush();
                            nullRecBQ.put(Arrays.asList(recs).toString());
                        }
                    }

                    if (sell9p != null && sell9qtya != null){
                        String sell9px = new String(sell9p);
                        String sell9qty = new String(sell9qtya);
                        double sell9pxd=Double.parseDouble(sell9px);
                        if(sell9pxd==0.0 && sell9qty.substring(0, 1).compareTo("0") > 0){
                            String[] recs = { secid, row, mdtime, sell9px, sell9qty, "卖9成交价格为0，成交量不为0" };
                            //LOGGER.info("有成交价格为0，交易量不为0的记录：" + Arrays.asList(recs).toString());
                            // nullWriter.writeRecord(recs);
                            // nullWriter.flush();
                            nullRecBQ.put(Arrays.asList(recs).toString());
                        }
                    }

                    if (sell10p != null && sell10qtya != null){
                        String sell10px = new String(sell10p);
                        String sell10qty = new String(sell10qtya);
                        double sell10pxd=Double.parseDouble(sell10px);
                        if(sell10pxd==0.0 && sell10qty.substring(0, 1).compareTo("0") > 0){
                            String[] recs = { secid, row, mdtime, sell10px, sell10qty, "卖10成交价格为0，成交量不为0" };
                            //LOGGER.info("有成交价格为0，交易量不为0的记录：" + Arrays.asList(recs).toString());
                            // nullWriter.writeRecord(recs);
                            // nullWriter.flush();
                            nullRecBQ.put(Arrays.asList(recs).toString());
                        }
                    }




                    byte[] maxp = r.getValue(Bytes.toBytes("Detail"), Bytes.toBytes("MaxPx"));
                    byte[] minp = r.getValue(Bytes.toBytes("Detail"), Bytes.toBytes("MinPx"));
                    byte[] lastp = r.getValue(Bytes.toBytes("Detail"), Bytes.toBytes("LastPx"));
                    byte[] highP = r.getValue(Bytes.toBytes("Detail"), Bytes.toBytes("HighPx"));
                    byte[] lowP = r.getValue(Bytes.toBytes("Detail"), Bytes.toBytes("LowPx"));

                    byte[] oldTvalue = r.getValue(Bytes.toBytes("Detail"), Bytes.toBytes("TotalValueTrade"));
                    byte[] oldTVolume = r.getValue(Bytes.toBytes("Detail"), Bytes.toBytes("TotalVolumeTrade"));

                    BigDecimal lastPx = new BigDecimal(new String(lastp));

                    if (maxp != null && minp != null && lastp != null) {
                        BigDecimal maxPx = new BigDecimal(new String(maxp));
                        BigDecimal minPx = new BigDecimal(new String(minp));

                        if (lastPx.doubleValue() != 0.0 && (lastPx.doubleValue() > maxPx.doubleValue()
                                || lastPx.doubleValue() < minPx.doubleValue())) {
                            String[] recs = { secid, row, mdtime, "lastPx:" + lastPx.toString(),
                                    "minPx:" + minPx.toString(), "maxPx:" + maxPx.toString() };
                            //LOGGER.info("lastPx超出涨跌幅限制：" + Arrays.asList(recs).toString());
                            // nullWriter.writeRecord(recs);
                            // nullWriter.flush();
                            minMaxBQ.put(Arrays.asList(recs).toString());
                        }
                    }

                    if (highP != null && lowP != null && lastp != null) {
                        BigDecimal highPx = new BigDecimal(new String(highP));
                        BigDecimal lowPx = new BigDecimal(new String(lowP));
                        if (lastPx.doubleValue() > highPx.doubleValue() || lastPx.doubleValue() < lowPx.doubleValue()
                                || highPx.doubleValue() < lowPx.doubleValue()) {
                            String[] recs = { secid, row, mdtime, "lastPx:" + lastPx.toString(),
                                    "highPx:" + highPx.toString(), "lowPx:" + lowPx.toString() };


                            highLowBQ.put(Arrays.asList(recs).toString());
                        }
                    }


                    if (oldTVolume != null) {
                        BigDecimal bvolume = new BigDecimal(new String(oldTVolume));
                        long oldVlumeshou = bvolume.longValue();
                        if (lastVolume != 0.0 && lastVolume < oldVlumeshou) {
                            String[] recs = { secid, row, mdtime, "totalVolume:" + oldVlumeshou,
                                    "nextVolume:" + lastVolume, "time:" + lastTime, "lastKey:" + lastRowKey };

                            volumeBQ.put(Arrays.asList(recs).toString());

                        }
                    }

                    if (oldTvalue != null) {

                        BigDecimal bvalue = new BigDecimal(new String(oldTvalue));
                        long oldValueshou = bvalue.longValue();
                        if (lastValue != 0 && lastValue < oldValueshou) {
                            String[] recs = { secid, row, mdtime, "totalValue:" + oldValueshou,
                                    "nextValue:" + lastValue, "time:" + lastTime, "lastKey:" + lastRowKey };
                            //LOGGER.info("totalValue没有递增：" + Arrays.asList(recs).toString());
                            valueBQ.put(Arrays.asList(recs).toString());

                        }

                    }

                    if (highP != null) {
                        BigDecimal highPx = new BigDecimal(new String(highP));
                        double oldHighPx = highPx.longValue();
                        if (lasthighPx != 0 && lasthighPx < oldHighPx) {
                            String[] recs = { secid, row, mdtime, "highPx:" + oldHighPx + "nextHighPx:" + lasthighPx,
                                    "time:" + lastTime, "lastKey:" + lastRowKey };
                            //LOGGER.info("HighPx没有递增：" + Arrays.asList(recs).toString());
                            highBQ.put(Arrays.asList(recs).toString());
                        }
                    }

                    if (lowP != null) {
                        BigDecimal lowPx = new BigDecimal(new String(lowP));

                        double oldLowPx = lowPx.longValue();
                        if (lastlowPx != 0.0 && lastlowPx > oldLowPx) {
                            String[] recs = { secid, row, mdtime, "lowPx:" + oldLowPx, "nextlowPx:" + lastlowPx,
                                    "time:" + lastTime, "lastKey:" + lastRowKey };
                            //LOGGER.info("lowPx没有递减：" + Arrays.asList(recs).toString());
                            lowBQ.put(Arrays.asList(recs).toString());

                        }
                    }

                    if (highP != null) {
                        BigDecimal highPx = new BigDecimal(new String(highP));
                        double oldHighPx = highPx.longValue();
                        lasthighPx = oldHighPx;
                    }
                    if (lowP != null) {
                        BigDecimal lowPx = new BigDecimal(new String(lowP));
                        double oldLowPx = lowPx.longValue();
                        lastlowPx = oldLowPx;
                    }

                    if (oldTVolume != null) {
                        BigDecimal bvolume = new BigDecimal(new String(oldTVolume));
                        long oldVlumeshou = bvolume.longValue();
                        lastVolume = oldVlumeshou;
                    }
                    if (oldTvalue != null) {

                        BigDecimal bvalue = new BigDecimal(new String(oldTvalue));
                        long oldValueshou = bvalue.longValue();
                        lastValue = oldValueshou;
                    }
                    lastRowKey = row;
                    lastTime = mdtime;


                } // 一行记录不为空结束
            } // 一行记录结束
        }
        System.out.println("完成日期" + date + " prdid:" + prdid + "校验");
    }

    public static void writeRec() throws Exception {
        ExecutorService exe = Executors.newFixedThreadPool(7);



        exe.submit(new Runnable() {
            /**
             * @see java.lang.Runnable#run()
             */
            @Override
            public void run() {
                // TODO Auto-generated method stub

                while (true) {
                    try {
                        String str = nullRecBQ.take();
                        nullWriter.write(str);
                        nullWriter.endRecord();
                        nullWriter.flush();
                    } catch (Exception e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                    }
                }
            }
        });

        exe.submit(new Runnable() {
            /**
             * @see java.lang.Runnable#run()
             */
            @Override
            public void run() {
                // TODO Auto-generated method stub

                while (true) {
                    try {
                        String str = highLowBQ.take();

                        highLowWriter.write(str);
                        highLowWriter.endRecord();
                        highLowWriter.flush();
                    } catch (Exception e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                    }
                }
            }
        });

        exe.submit(new Runnable() {
            /**
             * @see java.lang.Runnable#run()
             */
            @Override
            public void run() {
                // TODO Auto-generated method stub

                while (true) {
                    try {
                        String str = minMaxBQ.take();

                        minMaxWriter.write(str);
                        minMaxWriter.endRecord();
                        minMaxWriter.flush();
                    } catch (Exception e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                    }
                }
            }
        });

        exe.submit(new Runnable() {
            /**
             * @see java.lang.Runnable#run()
             */
            @Override
            public void run() {
                // TODO Auto-generated method stub

                while (true) {
                    try {
                        String str = highBQ.take();

                        highWriter.write(str);
                        highWriter.endRecord();
                        highWriter.flush();
                    } catch (Exception e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                    }
                }
            }
        });

        exe.submit(new Runnable() {
            /**
             * @see java.lang.Runnable#run()
             */
            @Override
            public void run() {
                // TODO Auto-generated method stub

                while (true) {
                    try {
                        String str = lowBQ.take();

                        lowWriter.write(str);
                        lowWriter.endRecord();
                        lowWriter.flush();
                    } catch (Exception e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                    }
                }
            }
        });

        exe.submit(new Runnable() {
            /**
             * @see java.lang.Runnable#run()
             */
            @Override
            public void run() {
                // TODO Auto-generated method stub

                while (true) {
                    try {
                        String str = volumeBQ.take();

                        volumeWriter.write(str);
                        volumeWriter.endRecord();
                        volumeWriter.flush();
                    } catch (Exception e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                    }
                }
            }
        });

        exe.submit(new Runnable() {
            /**
             * @see java.lang.Runnable#run()
             */
            @Override
            public void run() {
                // TODO Auto-generated method stub

                while (true) {
                    try {
                        String str = valueBQ.take();

                        valueWriter.write(str);
                        valueWriter.endRecord();
                        valueWriter.flush();
                    } catch (Exception e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                    }
                }
            }
        });

    }

}
