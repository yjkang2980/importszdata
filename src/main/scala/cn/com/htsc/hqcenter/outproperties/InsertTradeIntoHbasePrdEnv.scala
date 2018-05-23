package cn.com.htsc.hqcenter.outproperties

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
  *
  * @author
  * @version $Id:
  *
  */
object InsertTradeIntoHbasePrdEnv {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("hqdataimport")
    val sc = new SparkContext(conf)
    println("1-------------------------------------")
    val hiveContext = new HiveContext(sc)
    hiveContext.sql("use mdc")
    val sqlContext = new SQLContext(sc)
    val rb = ResourceBundle.getBundle("prdInfo".trim)
    val rb1=ResourceBundle.getBundle("option")
    val keys=rb.getKeys
    val prdinfoRdd=sc.textFile("hdfs://nameservice1/user/u010571/data/prdInfo.txt")

  //  val newprd=prdinfoRdd.map(row=>{
   // Row(row.split(",")(0).split("\\.",2)(0),row.split(",")(1),row.split(",")(2),row.split(",")(3))
    //})
   val newprd = prdinfoRdd.map(row => {
     Row(row.split(",")(0), row.split(",")(1), row.split(",")(2), row.split(",")(3))
   }).filter(row => {
     row(0).toString.endsWith(".SZ")
   }).map(row => {
     Row(row(0).toString.split("\\.", 2)(0), row(1), row(2), row(3))
   })

    val structType = StructType(Array(
      StructField("prdtid", StringType, true),
      StructField("sectype", StringType, true),
      StructField("secsubtype", StringType, true),
      StructField("symbol", StringType, true)
    ))
    val prdDF=hiveContext.createDataFrame(newprd, structType)

    val hdfs = org.apache.hadoop.fs.FileSystem.get(new java.net.URI("hdfs://nameservice1"), new org.apache.hadoop.conf.Configuration())

    val snapfls: Array[FileStatus] = hdfs.listStatus(new Path("hdfs://nameservice1/user/u010571/mddata/snapshotnew"))
    val days=getDayList(hiveContext,hdfs,snapfls)
    val startDay=args(0)
    val endDay=args(1)
    val startt=args(2)
    val endt=args(3)
    val id=args(4)
    for(day <- days){
      if(day >=startDay && day <=endDay){
        val year=day.substring(0,4)
        val mon=day.substring(4,6)
        val daye=day.substring(6)
       // insertEvertDay(hiveContext,prdDF,year,month,daye,zkQurom)
        println("开始导入"+year+"/"+mon+"/"+daye+"的数据")
        val sstart=year+mon+daye+startt
        val sendt=year+mon+daye+endt

        val indexSql="select t.origtime,t.securityid,t.mdstreamid,t.applseqnum,t.bidapplseqnum,t.offerapplseqnum, " +
          "case t.exectype when '4' then 1 when 'F' then 0 end as tradetype," +
          "t.price,t.tradeqty " +
          "from table_app_t_md_trade t where " +
          "t.year='"+year+"' and t.month='"+mon+"' and t.day='"+daye+"' and t.price>=0.0 and t.origtime between '"+sstart+"' and '"+sendt+"'"
        println("开始导入"+year+"/"+mon+"/"+daye+"的数据"+" 起止时间:"+sstart+"--"+sendt+"\n sql:"+indexSql)
        //定义 HBase 的配置
      //  val hbaseconf = HBaseConfiguration.create()
      //  hbaseconf.set("hbase.zookeeper.property.clientPort", "2181")
      //  hbaseconf.set("hbase.zookeeper.quorum", zkQurom)
        sc.hadoopConfiguration.set("hbase.zookeeper.quorum","arch-bd-zookeeper2,arch-bd-zookeeper4,arch-bd-zookeeper1,arch-bd-zookeeper3,arch-bd-zookeeper5")
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
        println("sql:"+indexSql)
        ///println(hiveContext.sql(indexSql).head(1))
        val ldDF=hiveContext.sql(indexSql)
        println("5----------------------------------count:"+ldDF.count())
        val midStockDF = ldDF.join(prdDF,ldDF("securityid")===prdDF("prdtid"))
        println("6--------------------------------join-after-count:"+midStockDF.count())
        //  val localData = hiveContext.sql(selectSnapSql).map(convertStockFundType(_,2))
        val localData=midStockDF.map(convertTradeType)

        //localData.saveAsHadoopDataset(jobConf)
        if(id.equals("T")){
          localData.saveAsNewAPIHadoopDataset(jobConf.getConfiguration)
        }else{
          val firstRec = midStockDF.head(1)(0)
          val format = new SimpleDateFormat("yyyyMMddHHmmssSSS")
          val secid = firstRec.getString(1)
          //val MDDate = firstRec.getString(0).substring(0, 8);
          val origt = format.parse(firstRec.getString(0))
          val HTSCSecurityID = secid + ".SZ"
          val applseqnum=firstRec.getLong(3)
          val rowKey: String = MD5Hash.getMD5AsHex(HTSCSecurityID.getBytes()).substring(0, 6) + HTSCSecurityID + (java.lang.Long.MAX_VALUE - origt.getTime)+
            ( java.lang.Long.MAX_VALUE-applseqnum)
          println("第一个记录：time:" + firstRec.getString(0) + " sec:" + HTSCSecurityID + " rowKey:" + rowKey)
        }

        println("导入"+year+"/"+mon+"/"+daye+"的trade指数数据结束")

      }
  }
    sc.stop()
  }

  def convertTradeType(row: Row)= {
    val format=new SimpleDateFormat("yyyyMMddHHmmssSSS")
    val secid=row.getAs[String]("securityid")
    val HTSCSecurityID=secid+".SZ"
    val origtime=row.getAs[String]("origtime").substring(8);
    val MDDate= row.getAs[String]("origtime").substring(0,8);
    val origt=format.parse(row.getAs[String]("origtime"))
    val applseqnum=row.getAs[Long]("applseqnum")
    val rowKey:String=MD5Hash.getMD5AsHex(HTSCSecurityID.getBytes).substring(0,6)+HTSCSecurityID+
      (java.lang.Long.MAX_VALUE-origt.getTime)+( java.lang.Long.MAX_VALUE-applseqnum);
    val sectype=row.getAs[String]("sectype")
    val subType=row.getAs[String]("secsubtype")
    val symbol=row.getAs[String]("symbol")
    val mdstreamid=row.getAs[String]("mdstreamid")
    val p = new Put(Bytes.toBytes(rowKey))

    val tradetype=row.getAs[Int]("tradetype")
    val price=row.getAs[Double]("price")
    val tradeqty=row.getAs[Long]("tradeqty")
    val bidapplseqnum=row.getAs[Long]("bidapplseqnum")
    val offerapplseqnum=row.getAs[Long]("offerapplseqnum")
    val df1:java.text.DecimalFormat=new java.text.DecimalFormat("######0.000")
    val totalM:Double=price * tradeqty;
    val tradeMoney=java.lang.Double.parseDouble(df1.format(totalM))

    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("MDDate"), Bytes.toBytes(MDDate))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("MDTime"), Bytes.toBytes(origtime))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("MDStreamID"), Bytes.toBytes(mdstreamid))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("SecurityType"), Bytes.toBytes(sectype))
    if("-"!=subType) {
      p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("SecuritySubType"), Bytes.toBytes(subType))
    }
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("SecurityID"), Bytes.toBytes(secid))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("SecurityIDSource"), Bytes.toBytes("102"))
    if("-"!=symbol) {
      p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("Symbol"), Bytes.toBytes(symbol))
    }
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("MDLevel"), Bytes.toBytes(1))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("MDChannel"), Bytes.toBytes(4))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("MDRecordType"), Bytes.toBytes(2))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("HTSCSecurityID"), Bytes.toBytes(HTSCSecurityID))

    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("TradeIndex"), Bytes.toBytes(applseqnum))
    //  p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("NumTrades"), Bytes.toBytes(numtrades))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("TradeBuyNo"), Bytes.toBytes(bidapplseqnum))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("TradeSellNo"), Bytes.toBytes(offerapplseqnum))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("TradeType"), Bytes.toBytes(tradetype))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("TradePrice"), Bytes.toBytes(price))
    // p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("ClosePx"), Bytes.toBytes())
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("TradeQty"), Bytes.toBytes(tradeqty))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("TradeMoney"), Bytes.toBytes(tradeMoney))
    (new ImmutableBytesWritable, p)

  }

  /**
    * 校验每一天的数据是否符合逻辑
    * @param hiveContext
    * @param hdfs
    * @param p1  example: val snapfls: Array[FileStatus] = hdfs.listStatus(new Path("hdfs://nameservice1/user/u010571/snapshotnew"))
    */
  def getDayList(hiveContext: HiveContext,hdfs: FileSystem,p1: Array[FileStatus]):List[String] = {
    var days: List[String] = List()
    for (fs <- p1) {
      val yearPathStr = fs.getPath.toString;
      val yearstr = yearPathStr.substring(yearPathStr.length - 4, yearPathStr.length)
      //val monthlist=hdfs.listStatus(sy.getPath)
      //println(yearPathStr)
      val monlist = hdfs.listStatus(fs.getPath)
      for (sy <- monlist) {
        val monPathStr = sy.getPath.toString;
        val monstr = monPathStr.substring(monPathStr.length - 2, monPathStr.length)
        val daylist = hdfs.listStatus(sy.getPath)
        for (day <- daylist) {
          val dayPathStr = day.getPath.toString;
          val daystr = dayPathStr.substring(dayPathStr.length - 2, dayPathStr.length)
          println("分区:" + "Y:" + yearstr + " M:" + monstr + " D:" + daystr)
          val location = yearstr  + monstr + daystr;
          days = location +: days
        }
      }
    }
    days
  }

}
