package cn.com.htsc.hqcenter.outproperties

import java.text.SimpleDateFormat
import java.util.ResourceBundle

import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.hbase.util.{Bytes, MD5Hash}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.hbase.client.Result
/**
  *
  * @author
  * @version $Id:
  *
  *
  */
object InsetIndexIntoHBase {

  def main(args: Array[String]): Unit = {
 val conf = new SparkConf().setAppName("hqdataimport")
    val sc = new SparkContext(conf)
    println("1-------------------------------------")
    val hiveContext = new HiveContext(sc)
    hiveContext.sql("use ana_crmpicture")
    val sqlContext = new SQLContext(sc)
    val rb = ResourceBundle.getBundle("prdInfo".trim)
    val rb1=ResourceBundle.getBundle("option")
    val zkQurom=rb1.getString("zkQurom")
    val keys=rb.getKeys
    val prdinfoRdd=sc.textFile("hdfs://nameservice1/user/u010571/data/prdInfo.txt")

    val newprd=prdinfoRdd.map(row=>{
    Row(row.split(",")(0).split("\\.",2)(0),row.split(",")(1),row.split(",")(2),row.split(",")(3))
    })

    val structType = StructType(Array(
      StructField("prdtid", StringType, true),
      StructField("sectype", StringType, true),
      StructField("secsubtype", StringType, true),
      StructField("symbol", StringType, true)
    ))
    val prdDF=hiveContext.createDataFrame(newprd, structType)

    println("2-----------------zk:--------------------"+zkQurom)

    val hdfs = org.apache.hadoop.fs.FileSystem.get(new java.net.URI("hdfs://nameservice1"), new org.apache.hadoop.conf.Configuration())

    val snapfls: Array[FileStatus] = hdfs.listStatus(new Path("hdfs://nameservice1/user/u010571/snapshotnew"))
    val days=getDayList(hiveContext,hdfs,snapfls)
    val startDay=args(0)
    val endDay=args(1)
    for(day <- days){
      if(day >=startDay && day <=endDay){
        val year=day.substring(0,4)
        val mon=day.substring(4,6)
        val daye=day.substring(6)
       // insertEvertDay(hiveContext,prdDF,year,month,daye,zkQurom)
        println("开始导入"+year+"/"+mon+"/"+daye+"的数据")
        hiveContext.sql("use ana_crmpicture")
        val indexSql="select t.origtime,t.securityid,t.securityidsource,t.mdstreamid, " +
          "t.tradingphasecode, t.preclosepx,t.totalvolumetrade," +
          "t.totalvaluetrade,t.lastpx,t.openpx,t.highpx,t.lowpx from table_app_t_md_index t where" +
          "  t.year='"+year+"' and t.month='"+mon+"' and t.day='"+daye+"' "

        //定义 HBase 的配置
      //  val hbaseconf = HBaseConfiguration.create()
      //  hbaseconf.set("hbase.zookeeper.property.clientPort", "2181")
      //  hbaseconf.set("hbase.zookeeper.quorum", zkQurom)
        sc.hadoopConfiguration.set("hbase.zookeeper.quorum",zkQurom)
        sc.hadoopConfiguration.set("hbase.zookeeper.property.clientPort", "2181")
        sc.hadoopConfiguration.set(TableOutputFormat.OUTPUT_TABLE, "MDC:MDIndexRecord")
        println("3-------------------------------------")
        //指定输出格式和输出表名
        //val jobConf = new JobConf(hbaseconf, this.getClass)
        val jobConf = new Job(sc.hadoopConfiguration)
        jobConf.setOutputKeyClass(classOf[ImmutableBytesWritable])
        jobConf.setOutputValueClass(classOf[Result])
        jobConf.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])
      //  jobConf.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])
        //  jobConf.set(TableOutputFormat.OUTPUT_TABLE, "MDC:MDIndexRecord")
        println("4-------------------------------------")
        println("sql:"+indexSql)
        ///println(hiveContext.sql(indexSql).head(1))
        val ldDF=hiveContext.sql(indexSql)
        println("5----------------------------------count:"+ldDF.count())
        val midStockDF = ldDF.join(prdDF,ldDF("securityid")===prdDF("prdtid"))
        println("6--------------------------------join-after-count:"+midStockDF.count())
        //  val localData = hiveContext.sql(selectSnapSql).map(convertStockFundType(_,2))
        val localData=midStockDF.map(convertIndexType)

        //localData.saveAsHadoopDataset(jobConf)
        localData.saveAsNewAPIHadoopDataset(jobConf.getConfiguration)
        println("导入"+year+"/"+mon+"/"+daye+"的数据结束")

      }
    }
    sc.stop()

  }



  def convertIndexType(row: Row)= {
    val format=new SimpleDateFormat("yyyyMMddHHmmssSSS")
    val secid=row.getAs[String]("securityid")
    val HTSCSecurityID=secid+".SZ"
    val origtime=row.getAs[String]("origtime").substring(8);
    val MDDate= row.getAs[String]("origtime").substring(0,8);
    val origt=format.parse(row.getAs[String]("origtime"))
    val rowKey:String=MD5Hash.getMD5AsHex(HTSCSecurityID.getBytes).substring(0,6)+HTSCSecurityID+(java.lang.Long.MAX_VALUE-origt.getTime)
    val sectype=row.getAs[String]("sectype")
    val subType=row.getAs[String]("secsubtype")
    val symbol=row.getAs[String]("symbol")
    val mdstreamid=row.getAs[String]("mdstreamid")
    val p = new Put(Bytes.toBytes(rowKey))
    val trphaseCode=row.getAs[String]("tradingphasecode").trim

    var tpc:String=
      trphaseCode match{
        case "S0" =>"0";case "O0" =>"1";case "T0" =>"3";case "C0" =>"5";case "E0" =>"6";case "A0"=>"7";case "H0"=>"8";case "V0"=>"9";
        case "S1"=>"8";case "O1"=>"8";case "B1"=>"8";case "T1"=>"8";case "C1"=>"8";case "E1"=>"8";
        case "A1"=>"8";case "H1"=>"8";case  "V1"=>"8";
        case "S"=>"0";case "O"=>"1";case "T"=>"3";case "C"=>"5";
        case "E"=>"6";case "A"=>"7";case "H"=>"8";case "V"=>"9";
      };
    val hm=row.getAs[String]("origtime").substring(8,12)
    if((hm>="0925" && hm<="0930")){
      tpc="2"
    }
    if((hm>="1130" && hm<="1300")){
      tpc="4"
    }
    val preclosepx=row.getAs[Double]("preclosepx")
    val totalVolumnTrade=row.getAs[Long]("totalvolumetrade")
    val totalValueTrade=row.getAs[Double]("totalvaluetrade")
    val lastpx=row.getAs[Double]("lastpx")
    val openpx=row.getAs[Double]("openpx")
    val highpx=row.getAs[Double]("highpx")
    val lowpx=java.lang.Double.parseDouble(row.getAs[String]("lowpx"))

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
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("MDRecordType"), Bytes.toBytes(1))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("TradingPhaseCode"), Bytes.toBytes(tpc))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("PreClosePx"), Bytes.toBytes(preclosepx))
  //  p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("NumTrades"), Bytes.toBytes(numtrades))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("TotalVolumeTrade"), Bytes.toBytes(totalVolumnTrade))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("TotalValueTrade"), Bytes.toBytes(totalValueTrade))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("LastPx"), Bytes.toBytes(lastpx))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("OpenPx"), Bytes.toBytes(openpx))
    // p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("ClosePx"), Bytes.toBytes())
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("HighPx"), Bytes.toBytes(highpx))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("LowPx"), Bytes.toBytes(lowpx))

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
