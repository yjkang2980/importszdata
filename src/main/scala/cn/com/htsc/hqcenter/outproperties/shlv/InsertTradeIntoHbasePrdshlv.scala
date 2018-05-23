package cn.com.htsc.hqcenter.outproperties.shlv

import java.text.SimpleDateFormat
import java.util.ResourceBundle

import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.apache.hadoop.hbase.client.{Put, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.hbase.util.{Bytes, MD5Hash}
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 上交所离线文件插入逐笔成交程序，参数形式：20140102 20140130 T T
  * 第一个参数代表批量插入的开始日期
  * 第二个参数代表批量插入的结束日期
  * 第三个参数代表是否插入，T代表插入，F代表不插入，仅会将第一条需要展示的数据打印出来
  * 第四个参数代表是否仅预处理T代表仅预处理,
  * @author
  * @version $Id:
  *
  */
object InsertTradeIntoHbasePrdshlv {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("hqshlv2tradedataimport")
    val sc = new SparkContext(conf)
    //println("1-------------------------------------")
    val hiveContext = new HiveContext(sc)
    //println("2-------------------------------------")
    hiveContext.sql("use mdc")
    //println("3-------------------------------------")
   // val sqlContext = new SQLContext(sc)
    val prdinfoRdd = sc.textFile("hdfs://nameservice1/user/u010571/data/prdInfo.txt")
   // println("4-------------------------------------")
    val newprd = prdinfoRdd.map(row => {
      Row(row.split(",")(0), row.split(",")(1), row.split(",")(2), row.split(",")(3))
    }).filter(row => {
      row(0).toString.endsWith(".SH")
    }).map(row => {
      Row(row(0).toString.split("\\.", 2)(0), row(1), row(2), row(3))
    })
    //println("5-------------------------------------")
    val structType = StructType(Array(
      StructField("prdtid", StringType, true),
      StructField("sectype", StringType, true),
      StructField("secsubtype", StringType, true),
      StructField("symbol", StringType, true)
    ))
    val prdDF = hiveContext.createDataFrame(newprd, structType)
    //println("6-------------------------------------")
    var confh=new org.apache.hadoop.conf.Configuration();
    //println("7-------------------------------------")

    val hdfs = org.apache.hadoop.fs.FileSystem.get(new java.net.URI("hdfs://nameservice1"), confh)
    val snapfls: Array[FileStatus] = hdfs.listStatus(new Path("hdfs://nameservice1/user/u010571/shlv2/tradeticker"))
    val origFiles:Array[FileStatus] = hdfs.listStatus(new Path("hdfs://nameservice1/user/u010571/History_data/his_sh2"))
    val days = getDayList(hiveContext, hdfs, snapfls)
    val startDay = args(0)
    val endDay = args(1)

    for (origf <- origFiles){
      val ymd=origf.getPath.toString.substring(origf.getPath.toString.length-8,origf.getPath.toString.length);
      if(ymd>=startDay && ymd<=endDay){
        val tickFile=new Path(origf.getPath.toString+"/Tick.csv");
        if(hdfs.exists(tickFile)) {
          // val tickFile=new Path(origf.getPath.toString+"/Auction.csv");
          val descTick = new Path("hdfs://nameservice1/user/u010571/shlv2/tradeticker/" + ymd + "/" + "Tick.csv");
          //val descTick=new Path("hdfs://nameservice1/user/u010571/shlv2/tradeticker/"+ymd+"/"+"Auction.csv");
          var p = new Path("hdfs://nameservice1/user/u010571/shlv2/tradeticker/" + ymd);
          if (!hdfs.exists(p)) {
            hdfs.mkdirs(p);
          }
          if (hdfs.exists(descTick)) {
            hdfs.delete(descTick, true)
          }
          //println(snapFile+" \n " + descSnap)
          //hdfs.rename(snapFile,descSnap)
          hdfs.rename(tickFile, descTick)
          val dropp = "ALTER TABLE table_app_t_md_trade_shlv_orig  DROP IF EXISTS PARTITION (ymd='" + ymd + "')"
          println(" 如果已有删除分区： " + ymd)
          hiveContext.sql(dropp)
          val strsql = "alter table table_app_t_md_trade_shlv_orig add partition (ymd='" + ymd + "') location '" + ymd + "'"
          println("分区语句：" + strsql)
          hiveContext.sql(strsql)
          println("已完成分区添加!" + "分区:" + ymd)
        }
      }
    }
    if(null!=args(3) && args(3).equals("T")){
      println("第四个参数为T，代表仅做数据预处理：移动文件到指定位置，建立hive外表分区映射")
      System.exit(1)
    }

    val insertData = args(2)
    //val startt=args(2)
    //val endt=args(3)
    for (day <- days) {
      if (day >= startDay && day <= endDay) {
        val year = day.substring(0, 4)
        val mon = day.substring(4, 6)
        val daye = day.substring(6)
        // insertEvertDay(hiveContext,prdDF,year,month,daye,zkQurom)
        //println("开始导入" + year + "/" + mon + "/" + daye + "的数据")
        //   val sstart=year+mon+daye+startt
        //  val sendt=year+mon+daye+endt

        val tradesql = "select *,row_number() over (partition by securityid ORDER BY tradetime asc) as rownum from table_app_t_md_trade_shlv_orig t where t.ymd='"+day+"' and t.securityid!='HEAD:v1' "

        println("开始导入" + year + "/" + mon + "/" + daye + "的数据" + " sql:" + tradesql)
        // println("开始导入"+year+"/"+mon+"/"+daye+"的数据"l)
        //定义 HBase 的配置
        //  val hbaseconf = HBaseConfiguration.create()
        //  hbaseconf.set("hbase.zookeeper.property.clientPort", "2181")
        //  hbaseconf.set("hbase.zookeeper.quorum", zkQurom)
        sc.hadoopConfiguration.set("hbase.zookeeper.quorum", "arch-bd-zookeeper2,arch-bd-zookeeper4,arch-bd-zookeeper1,arch-bd-zookeeper3,arch-bd-zookeeper5")
        sc.hadoopConfiguration.set("hbase.zookeeper.property.clientPort", "2181")
        sc.hadoopConfiguration.set(TableOutputFormat.OUTPUT_TABLE, "MDC:MDTransactionRecord")
        //指定输出格式和输出表名
        //val jobConf = new JobConf(hbaseconf, this.getClass)
        val jobConf = new Job(sc.hadoopConfiguration)
        jobConf.setOutputKeyClass(classOf[ImmutableBytesWritable])
        jobConf.setOutputValueClass(classOf[Result])
        jobConf.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])
        //  jobConf.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])
        //  jobConf.set(TableOutputFormat.OUTPUT_TABLE, "MDC:MDIndexRecord")
        // println("sql:"+indexSql)
        ///println(hiveContext.sql(indexSql).head(1))
        val ldDF = hiveContext.sql(tradesql)
       // println("5----------------------------------count:" + ldDF.count())
        var midStockDF = ldDF.join(prdDF, ldDF("securityid") === prdDF("prdtid"))
        //midStockDF.persist().count()
       // midStockDF=midStockDF.filter(midStockDF("prdtid").equalTo("600084")).filter(midStockDF("tradetime").equalTo("2014010209250403")).filter(midStockDF("rownum").equalTo(1));

        //println("6--------------------------------join-after-count:" + midStockDF.count())
        //  val localData = hiveContext.sql(selectSnapSql).map(convertStockFundType(_,2))
        val localData = midStockDF.map(convertTradeType)
        //localData.persist()
        localData.saveAsNewAPIHadoopDataset(jobConf.getConfiguration)
        val c1=localData.count()
        if (insertData.equals("T")) {
          //localData.saveAsHadoopDataset(jobConf)
          println("6--------------------------------导入:"+day+" - " + c1+" 条逐笔成交数据....")
          localData.saveAsNewAPIHadoopDataset(jobConf.getConfiguration)
        } else {
          val sa=midStockDF.head(1)(0)
          val tradeDate = sa.getAs[String]("tradetime")
          val format=new SimpleDateFormat("yyyyMMddHHmmssSS")
          //  val tradeDatetimestamp=sa(1).toString.length==8?("0"+sa(1).toString):sa(1)
          val sec=sa.getAs[String]("securityid")+".SH"
          var origtime=sa.getAs[String]("tradetime")
          val recno=sa.getAs[Long]("rownum")

          val origt=format.parse(origtime)
          val rowKey: String = MD5Hash.getMD5AsHex(sec.getBytes).substring(0, 6) + sec +
            (java.lang.Long.MAX_VALUE - origt.getTime) + (java.lang.Long.MAX_VALUE - recno);
          println("第一个记录：time:"+origtime+" sec:"+sec+" recno:"+recno+" rowKey:"+rowKey)
        }
        println("导入" + year + "/" + mon + "/" + daye + "的trade指数数据结束,共计插入条数:"+c1)

      }
    }
    sc.stop()
  }

  def convertTradeType(row: Row) = {
    val format = new SimpleDateFormat("yyyyMMddHHmmssSS")
    val secid = row.getAs[String]("securityid")
    val HTSCSecurityID = secid+".SH"

    val MDDate = row.getAs[String]("tradetime").substring(0, 8);
    var mdtime = row.getAs[String]("tradetime").substring(8);

    val origtime1 = MDDate + mdtime

    val origt = format.parse(origtime1)

    val applseqnum = row.getAs[Integer]("rownum")
   // println("数据特征："+origtime1+" secid:"+secid+" num:"+applseqnum)
    val rowKey: String = MD5Hash.getMD5AsHex(HTSCSecurityID.getBytes).substring(0, 6) + HTSCSecurityID +
      (java.lang.Long.MAX_VALUE - origt.getTime) + (java.lang.Long.MAX_VALUE - applseqnum);
    val sectype = row.getAs[String]("sectype")
    val subType = row.getAs[String]("secsubtype")
    val symbol = row.getAs[String]("symbol")
    //val mdstreamid=row.getAs[String]("mdstreamid")
    val p = new Put(Bytes.toBytes(rowKey))
    val bidapplseqnum = java.lang.Long.parseLong(row.getAs[String]("bidapplseqnum"))
    val offerapplseqnum = java.lang.Long.parseLong(row.getAs[String]("offerapplseqnum"))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("TradeType"), Bytes.toBytes("0"))
    //var tradetype = row.getAs[String]("functioncode").trim
    //var orderkind=row.getAs[String]("orderkind").trim
    if(bidapplseqnum>offerapplseqnum){
      p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("TradeBSFlag"), Bytes.toBytes("1"))
    }
    if(bidapplseqnum<offerapplseqnum){
      p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("TradeBSFlag"), Bytes.toBytes("2"))
    }
    //if (tradetype.equals("0") && orderkind.equals("0")) {
     // p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("TradeType"), Bytes.toBytes("0"))
   // } else if (tradetype.equals("C") && orderkind.equals("0")) {
   //   p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("TradeType"), Bytes.toBytes("1"))
   // }else if(orderkind.equals("2") && tradetype.equals("C")){
   //   p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("TradeType"), Bytes.toBytes("2"))
   // }
    val price = row.getAs[String]("price")
    val tradeqty = row.getAs[String]("tradeqty")

    //val df1: java.text.DecimalFormat = new java.text.DecimalFormat("######0.000")
    //val totalM: Double = new java.math.BigDecimal(price).multiply(new java.math.BigDecimal(tradeqty)).doubleValue();

    val tradeMoney =  row.getAs[String]("tradeamount")

    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("MDDate"), Bytes.toBytes(MDDate))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("ReceiveDateTime"), Bytes.toBytes(row.getAs[String]("tradetime")))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("MDTime"), Bytes.toBytes(mdtime))
    //  p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("MDStreamID"), Bytes.toBytes(mdstreamid))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("SecurityType"), Bytes.toBytes(sectype))
    if ("-" != subType) {
      p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("SecuritySubType"), Bytes.toBytes(subType))
    }
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("SecurityID"), Bytes.toBytes(secid))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("SecurityIDSource"), Bytes.toBytes("101"))
    if ("-" != symbol) {
      p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("Symbol"), Bytes.toBytes(symbol))
    }
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("MDLevel"), Bytes.toBytes("1"))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("MDChannel"), Bytes.toBytes("3"))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("MDRecordType"), Bytes.toBytes("2"))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("HTSCSecurityID"), Bytes.toBytes(HTSCSecurityID))

    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("TradeIndex"), Bytes.toBytes(applseqnum.toString))
    //  p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("NumTrades"), Bytes.toBytes(numtrades))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("TradeBuyNo"), Bytes.toBytes(bidapplseqnum.toString))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("TradeSellNo"), Bytes.toBytes(offerapplseqnum.toString))
    //p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("TradeType"), Bytes.toBytes(tradetype))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("TradePrice"), Bytes.toBytes(price.toString))
    // p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("ClosePx"), Bytes.toBytes())
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("TradeQty"), Bytes.toBytes(tradeqty.toString))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("TradeMoney"), Bytes.toBytes(tradeMoney.toString))
    (new ImmutableBytesWritable, p)

  }

  /**
    * 校验每一天的数据是否符合逻辑
    *
    * @param hiveContext
    * @param hdfs
    * @param p1 example: val snapfls: Array[FileStatus] = hdfs.listStatus(new Path("hdfs://nameservice1/user/u010571/snapshotnew"))
    */
  def getDayList(hiveContext: HiveContext, hdfs: FileSystem, p1: Array[FileStatus]): List[String] = {
    var days: List[String] = List()
    for (fs <- p1) {
      val ymd = fs.getPath.toString;
      val yearstr = ymd.substring(ymd.length - 8, ymd.length)
          days = yearstr +: days
    }
    days
  }

}
