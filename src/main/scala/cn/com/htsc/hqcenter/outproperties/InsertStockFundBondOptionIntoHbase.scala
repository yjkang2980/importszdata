package cn.com.htsc.hqcenter.outproperties

import java.text.SimpleDateFormat
import java.util.ResourceBundle

import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Put, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.hbase.util.{Bytes, MD5Hash}
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{StringType, StructField, StructType}

/**
  *
  * @author
  * @version $Id:
  *
  */

/*
测试环境中数据导入新格试
 */
object InsertStockFundBondOptionIntoHbase {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("hqdataimport")
    val sc = new SparkContext(conf)
    println("1-------------------------------------")
    val hiveContext = new HiveContext(sc)
    hiveContext.sql("use ana_crmpicture")
    val sqlContext = new SQLContext(sc)
    val rb = ResourceBundle.getBundle("prdInfo".trim)
    val keys=rb.getKeys
    val rb1=ResourceBundle.getBundle("option")
    val zkQurom=rb1.getString("zkQurom")
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

    val hdfs = org.apache.hadoop.fs.FileSystem.get(new java.net.URI("hdfs://nameservice1"), new org.apache.hadoop.conf.Configuration())

    val snapfls: Array[FileStatus] = hdfs.listStatus(new Path("hdfs://nameservice1/user/u010571/snapshotnew"))
    val days=getDayList(hiveContext,hdfs,snapfls)
    val startDay=args(0)
    val endDay=args(1)
    for(day <- days){
      if(day >=startDay && day <=endDay){
        val year=day.substring(0,4)
        val month=day.substring(4,6)
        val daye=day.substring(6)
        println("开始导入"+year+"/"+month+"/"+daye+"的数据")
        hiveContext.sql("use ana_crmpicture")
        val selectSnapSql="select a.securityid,a.mdstreamid,a.origtime,a.preclosepx,a.pxchange1,a.pxchange2,a.openpx," +
          "a.highpx,a.lowpx,a.lastpx,a.numtrades,a.totalvolumetrade,a.totalvaluetrade,a.uplimitpx,a.downlimitpx," +
          "a.totalofferqty,a.totalbidqty,a.weightedavgbidpx,a.weightedavgofferpx,a.peratio1,a.peratio2,a.tradingphasecode,a.warrantpremiumrate," +
          "b.bidpx1,b.bidsize1,b.numorders_b1,b.noorders_b1,b.orderqty_b1," +
          "b.offerpx1,b.offersize1,b.numorders_s1,b.noorders_s1,b.orderqty_s1," +
          "b.offerpx2,b.offersize2,b.bidpx2,b.bidsize2,b.bidpx3,b.bidsize3,b.offerpx3,b.offersize3," +
          "b.offerpx4,b.offersize4,b.bidpx4,b.bidsize4,b.bidpx5,b.bidsize5,b.offerpx5,b.offersize5," +
          "b.offerpx6,b.offersize6,b.bidpx6,b.bidsize6,b.bidpx7,b.bidsize7,b.offerpx7,b.offersize7," +
          "b.offerpx8,b.offersize8,b.bidpx8,b.bidsize8,b.bidpx9,b.bidsize9,b.offerpx9,b.offersize9," +
          "b.offerpx10,b.offersize10,b.bidpx10,b.bidsize10 " +
          "from table_app_t_md_snap a,table_app_t_md_snap_level b where" +
          "   a.origtime=b.origtime and a.securityid=b.securityid and" +
          " a.year='"+year+"' and a.month='"+month+"' and a.day='"+daye+"' and a.year=b.year and a.month=b.month " +
          " and a.day=b.day "
        //and a.origtime='20160831101024000' and a.securityid='300448'

        sc.hadoopConfiguration.set("hbase.zookeeper.quorum",zkQurom)
        sc.hadoopConfiguration.set("hbase.zookeeper.property.clientPort", "2181")
        sc.hadoopConfiguration.set(TableOutputFormat.OUTPUT_TABLE, "MDC:MDStockRecord")
        //定义 HBase 的配置
        //val hbaseconf = HBaseConfiguration.create()
        //hbaseconf.set("hbase.zookeeper.property.clientPort", "2181")
        //hbaseconf.set("hbase.zookeeper.quorum", zkQurom)
        val jobConf = new Job(sc.hadoopConfiguration)
        jobConf.setOutputKeyClass(classOf[ImmutableBytesWritable])
        jobConf.setOutputValueClass(classOf[Result])
        jobConf.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])

        println("4-------------------------------------")
        println("sql:"+selectSnapSql)

        val ldDF=hiveContext.sql(selectSnapSql)
        println("5-----------------------count:"+ldDF.count())
        val midStockDF = ldDF.join(prdDF,ldDF("securityid")===prdDF("prdtid")).filter("sectype='2'")
        //  val localData = hiveContext.sql(selectSnapSql).map(convertStockFundType(_,2))
        val localData=midStockDF.map(convertStockFundType)
        println("6-------------------------localdataRdd-count:-"+localData.count())
       // println(hiveContext.sql(selectSnapSql).head(1))

        localData.saveAsNewAPIHadoopDataset(jobConf.getConfiguration)
        println("导入"+year+"/"+month+"/"+daye+"的数据结束")

      }
    }



    // public static final int IndexType_VALUE = 1;
   /// public static final int StockType_VALUE = 2;
    //public static final int FundType_VALUE = 3;
   // public static final int BondType_VALUE = 4;
    //public static final int RepoType_VALUE = 5;
    //public static final int WarrantType_VALUE = 6;
   // public static final int OptionType_VALUE = 7;
    //public static final int FuturesType_VALUE = 8;

    //指定输出格式和输出表名
    //val jobConf2 = new JobConf(hbaseconf, this.getClass)
   // jobConf2.setOutputFormat(classOf[TableOutputFormat])
   // jobConf2.set(TableOutputFormat.OUTPUT_TABLE, "MDC:MDFundRecord")
   // val localData2 = hiveContext.sql(selectSnapSql).map(convertStockFundType(_,3))

   // localData2.saveAsHadoopDataset(jobConf2)
  }

def insertSnapEveryDay(hiveContext: HiveContext,prdDF:DataFrame,year:String,mon:String,day:String,zkQurom:String): Unit ={

}
  /**
    * Step 2：RDD 到表模式的映射
    */
  def convertStockFundType(row: Row):(ImmutableBytesWritable,Put) = {
   // val rb = ResourceBundle.getBundle("prdInfo".trim)
    //val keys=rb.getKeys
    val format=new SimpleDateFormat("yyyyMMddHHmmssSSS")
    val secid=row.getString(0)
    val mdstreamid=row.getString(1);val origtime=row.getString(2).substring(8);val MDDate= row.getString(2).substring(0,8);val origt=format.parse(row.getString(2))
    val preclosepx=row.getDouble(3);val pxchange1=row.getDouble(4);val pxchange2=row.getDouble(5);val openpx=row.getDouble(6);
    val highpx=row.getDouble(7);val lowpx=row.getDouble(8);val lastpx=row.getDouble(9);val numtrades=row.getLong(10);
    val totalVolumnTrade=row.getLong(11);val totalValueTrade=row.getDouble(12);val uplimitpx=row.getDouble(13);val downlimitpx=row.getDouble(14);
    var totalofferqty=row.getString(15);var totalbidQty=row.getString(16);
    if(totalofferqty.contains(".")){
      totalofferqty=totalofferqty.split("\\.",2)(0)
    }
    if(totalbidQty.contains(".")){
      totalbidQty=totalbidQty.split("\\.",2)(0)
    }

    val weightebidpx=row.getDouble(17);val weiavgofferpx=row.getDouble(18)
    val peratio1=row.getDouble(19);val peratio2=row.getDouble(20);val trphaseCode=row.getString(21).trim; val warrantrate=row.getDouble(22);
    val bidpx1=row.getDouble(23);val bidsize1=row.getLong(24);val numorderb1=row.getLong(25);val noorderb1=row.getLong(26);
    val orderqtyb1=row.getString(27);
    val offerpx1=row.getDouble(28);val offersize1=row.getLong(29);val numorders1=row.getLong(30);val noorders1=row.getLong(31);
    val orderqtys1=row.getString(32);
    val offerpx2=row.getDouble(33);val offersize2=row.getLong(34);val bidpx2=row.getDouble(35);val bidsize2=row.getLong(36);
    val bidpx3=row.getDouble(37);val bidsize3=row.getLong(38);val offerpx3=row.getDouble(39);val offersize3=row.getLong(40);
    val offerpx4=row.getDouble(41);val offersize4=row.getLong(42);val bidpx4=row.getDouble(43);val bidsize4=row.getLong(44);
    val bidpx5=row.getDouble(45);val bidsize5=row.getLong(46);val offerpx5=row.getDouble(47);val offersize5=row.getLong(48);
    val offerpx6=row.getDouble(49);val offersize6=row.getLong(50);val bidpx6=row.getDouble(51);val bidsize6=row.getLong(52);
    val bidpx7=row.getDouble(53);val bidsize7=row.getLong(54);val offerpx7=row.getDouble(55);val offersize7=row.getLong(56);
    val offerpx8=row.getDouble(57);val offersize8=row.getLong(58);val bidpx8=row.getDouble(59);val bidsize8=row.getLong(60);
    val bidpx9=row.getDouble(61);val bidsize9=row.getLong(62);val offerpx9=row.getDouble(63);val offersize9=row.getLong(64);
    val offerpx10=row.getDouble(65);val offersize10=row.getLong(66);val bidpx10=row.getDouble(67);val bidsize10=row.getLong(68);

    println("-------------------77777------------------")
    var tpc:String=
      trphaseCode match{
        case "S0" =>"0";case "O0" =>"1";case "T0" =>"3";case "C0" =>"5";case "E0" =>"6";case "A0"=>"7";case "H0"=>"8";case "V0"=>"9";
        case "S1"=>"8";case "O1"=>"8";case "B1"=>"8";case "T1"=>"8";case "C1"=>"8";case "E1"=>"8";
        case "A1"=>"8";case "H1"=>"8";case  "V1"=>"8";
        case "S"=>"0";case "O"=>"1";case "T"=>"3";case "C"=>"5";case "E"=>"6";case "A"=>"7";case "H"=>"8";case "V"=>"9";
        case "B0"=>"4";
      };
   val hm=row.getString(2).substring(8,12)
    if((hm>="0925" && hm<="0930")){
      tpc="2"
    }
    if((hm>="1130" && hm<="1300")){
      tpc="4"
    }

    val HTSCSecurityID=secid+".SZ"
    val rowKey:String=MD5Hash.getMD5AsHex(HTSCSecurityID.getBytes()).substring(0,6)+HTSCSecurityID+(java.lang.Long.MAX_VALUE-origt.getTime)

    //val props = rb.getString(HTSCSecurityID)
    //val str = props.trim.split(",")
     val sectype=row.getString(70)
     val subType=row.getString(71)
      val symbol=row.getString(72)
      val p = new Put(Bytes.toBytes(rowKey))
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
      p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("NumTrades"), Bytes.toBytes(numtrades))
      p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("TotalVolumeTrade"), Bytes.toBytes(totalVolumnTrade))
      p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("TotalValueTrade"), Bytes.toBytes(totalValueTrade))
      p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("LastPx"), Bytes.toBytes(lastpx))
      p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("OpenPx"), Bytes.toBytes(openpx))
     // p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("ClosePx"), Bytes.toBytes())
      p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("HighPx"), Bytes.toBytes(highpx))
      p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("LowPx"), Bytes.toBytes(lowpx))
      p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("DiffPx1"), Bytes.toBytes(pxchange1))
      p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("DiffPx2"), Bytes.toBytes(pxchange2))
      p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("MaxPx"), Bytes.toBytes(uplimitpx))
      p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("MinPx"), Bytes.toBytes(downlimitpx))
      p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("TotalBidQty"), Bytes.toBytes(totalbidQty))
      p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("TotalOfferQty"), Bytes.toBytes(totalofferqty))
      p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("SLYOne"), Bytes.toBytes(peratio1))
      p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("SLYTwo"), Bytes.toBytes(peratio2))
      p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("Buy1Price"), Bytes.toBytes(bidpx1))
      p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("Buy1OrderQty"), Bytes.toBytes(bidsize1))
      p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("Buy1NumOrders"), Bytes.toBytes(numorderb1))
      p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("Buy1NoOrders"), Bytes.toBytes(noorderb1))
      p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("Buy1OrderDetail"), Bytes.toBytes(orderqtyb1))
      p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("Sell1Price"), Bytes.toBytes(offerpx1))
      p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("Sell1OrderQty"), Bytes.toBytes(offersize1))
      p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("Sell1NumOrders"), Bytes.toBytes(numorders1))
      p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("Sell1NoOrders"), Bytes.toBytes(noorders1))
      p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("Sell1OrderDetail"), Bytes.toBytes(orderqtys1))
      p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("Buy2Price"), Bytes.toBytes(bidpx2))
      p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("Buy2OrderQty"), Bytes.toBytes(bidsize2))
      p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("Sell2Price"), Bytes.toBytes(offerpx2))
      p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("Sell2OrderQty"), Bytes.toBytes(offersize2))
      p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("Buy3Price"), Bytes.toBytes(bidpx3))
      p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("Buy3OrderQty"), Bytes.toBytes(bidsize3))
      p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("Sell3Price"), Bytes.toBytes(offerpx3))
      p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("Sell3OrderQty"), Bytes.toBytes(offersize3))
      p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("Buy4Price"), Bytes.toBytes(bidpx4))
      p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("Buy4OrderQty"), Bytes.toBytes(bidsize4))
      p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("Sell4Price"), Bytes.toBytes(offerpx4))
      p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("Sell4OrderQty"), Bytes.toBytes(offersize4))
      p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("Buy5Price"), Bytes.toBytes(bidpx5))
      p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("Buy5OrderQty"), Bytes.toBytes(bidsize5))
      p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("Sell5Price"), Bytes.toBytes(offerpx5))
      p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("Sell5OrderQty"), Bytes.toBytes(offersize5))
      p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("Buy6Price"), Bytes.toBytes(bidpx6))
      p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("Buy6OrderQty"), Bytes.toBytes(bidsize6))
      p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("Buy7Price"), Bytes.toBytes(bidpx7))
      p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("Buy7OrderQty"), Bytes.toBytes(bidsize7))
      p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("Sell7Price"), Bytes.toBytes(offerpx7))
      p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("Sell7OrderQty"), Bytes.toBytes(offersize7))
      p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("Buy8Price"), Bytes.toBytes(bidpx8))
      p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("Buy8OrderQty"), Bytes.toBytes(bidsize8))
      p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("Sell8Price"), Bytes.toBytes(offerpx8))
      p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("Sell8OrderQty"), Bytes.toBytes(offersize8))
      p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("Buy9Price"), Bytes.toBytes(bidpx9))
      p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("Buy9OrderQty"), Bytes.toBytes(bidsize9))
      p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("Sell9Price"), Bytes.toBytes(offerpx9))
      p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("Sell9OrderQty"), Bytes.toBytes(offersize9))
      p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("Buy10Price"), Bytes.toBytes(bidpx10))
      p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("Buy10OrderQty"), Bytes.toBytes(bidsize10))
      p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("Sell10Price"), Bytes.toBytes(offerpx10))
      p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("Sell10OrderQty"), Bytes.toBytes(offersize10))

    //  println("返回stocktype："+typek+"--"+sectype)
      (new ImmutableBytesWritable, p)

  }


  def insertIntoSnapRecord(): Unit ={

  }



  def getNotExistsSecurityIds(sc:SparkContext, hiveContext: HiveContext,year:String,mon:String,day:String):List[String]={


    val snapfls ="hdfs://nameservice1/user/u010571/data/prdInfo.properties"
    var m:Map[String,String]=Map()
    val mapRdd=sc.textFile(snapfls).map(row=>Row(row.split("=")(0),row.split("=")(1)))
    mapRdd.foreach(
      row=>{
        println(row)
        m += (row.getString(0) ->row.getString(1))
      }
    )

    val numsecId="select a.securityid from table_app_t_md_snap a where a.year='"+year+"' and month='"+mon+"' and day='"+day+"'"

    val secids = hiveContext.sql(numsecId).distinct()
    var list:List[String] = List()
    secids.foreach(row=>{
      val str = row.getString(0)
      list= str::list
    })
    return list

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
