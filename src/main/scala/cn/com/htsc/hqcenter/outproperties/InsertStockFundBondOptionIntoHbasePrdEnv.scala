package cn.com.htsc.hqcenter.outproperties

import java.text.SimpleDateFormat
import java.util.ResourceBundle

import org.apache.commons.lang.StringUtils
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.apache.hadoop.hbase.client.{Put, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.hbase.util.{Bytes, MD5Hash}
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  *
  * @author
  * @version $Id: 生产环境中数据导入，新格试
  *
  */
// public static final int IndexType_VALUE = 1; 指数，
/// public static final int StockType_VALUE = 2; 股票......
//public static final int FundType_VALUE = 3; 基金.......
// public static final int BondType_VALUE = 4; 债券.....
//public static final int RepoType_VALUE = 5;  回购----------没有hbase表
//public static final int WarrantType_VALUE = 6; //权证(201608310915-1130 没有数据....)
// public static final int OptionType_VALUE = 7;  //期权(201608310915-1130 没有数据....)
//public static final int FuturesType_VALUE = 8;  //期货(201608310915-1130 没有数据....)

object InsertStockFundBondOptionIntoHbasePrdEnv {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("hqdataimport")
    val sc = new SparkContext(conf)
    println("1-------------------------------------")
    val hiveContext = new HiveContext(sc)
    hiveContext.sql("use mdc")
   // val sqlContext = new SQLContext(sc)
   // val rb = ResourceBundle.getBundle("prdInfo".trim)
   // val keys = rb.getKeys
   // val rb1 = ResourceBundle.getBundle("option")
    val prdinfoRdd = sc.textFile("hdfs://nameservice1/user/u010571/data/prdInfo.txt")
    //val newprd=prdinfoRdd.map(row=>{
    //Row(row.split(",")(0).split("\\.",2)(0),row.split(",")(1),row.split(",")(2),row.split(",")(3))
    // })

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
    val prdDF = hiveContext.createDataFrame(newprd, structType)

    prdDF.registerTempTable("prdDF")


    val hdfs = org.apache.hadoop.fs.FileSystem.get(new java.net.URI("hdfs://nameservice1"), new org.apache.hadoop.conf.Configuration())

    val snapfls: Array[FileStatus] = hdfs.listStatus(new Path("hdfs://nameservice1/user/u010571/mddata/snapshotnew"))
    val days = getDayList(hiveContext, hdfs, snapfls)
    val startDay = args(0)
    val endDay = args(1)
   // val startTime = args(2)
   // val endTime = args(3)
    val insertData = args(5)
    for (day <- days) {
      if (day >= startDay && day <= endDay) {
        val year = day.substring(0, 4)
        val month = day.substring(4, 6)
        val daye = day.substring(6)
        println("开始导入" + year + "/" + month + "/" + daye + "的数据")
        //val sstart = year + month + daye
        //val sendt = year + month + daye
        println("开始时间：" + startDay + " --结束时间：" + endDay + " 表名称：" + args(4) + ",类别代码：" + args(5))
        val selectSnapSql = "select a.securityid,a.mdstreamid,a.origtime,a.preclosepx,a.pxchange1,a.pxchange2,a.openpx," +
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
          " a.year='" + year + "' and a.month='" + month + "' and a.day='" + daye + "' and a.year=b.year and a.month=b.month " +
          " and a.day=b.day "
        //and a.origtime='20160831101024000' and a.securityid='300448'

        sc.hadoopConfiguration.set("hbase.zookeeper.quorum", "arch-bd-zookeeper2,arch-bd-zookeeper4,arch-bd-zookeeper1,arch-bd-zookeeper3,arch-bd-zookeeper5")
        sc.hadoopConfiguration.set("hbase.zookeeper.property.clientPort", "2181")
        sc.hadoopConfiguration.set(TableOutputFormat.OUTPUT_TABLE, args(4))
        //定义 HBase 的配置
        //val hbaseconf = HBaseConfiguration.create()
        //hbaseconf.set("hbase.zookeeper.property.clientPort", "2181")
        //hbaseconf.set("hbase.zookeeper.quorum", zkQurom)
        val jobConf = new Job(sc.hadoopConfiguration)
        jobConf.setOutputKeyClass(classOf[ImmutableBytesWritable])
        jobConf.setOutputValueClass(classOf[Result])
        jobConf.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])

        println("4-------------------------------------")
        println("sql:" + selectSnapSql)

        val ldDF = hiveContext.sql(selectSnapSql)
        println("5-----------------------count:" + ldDF.count())
        val midStockDF = ldDF.join(prdDF, ldDF("securityid") === prdDF("prdtid")).filter("sectype='" + args(5) + "'")
        midStockDF.persist();
        //  val localData = hiveContext.sql(selectSnapSql).map(convertStockFundType(_,2))
        val localData = midStockDF.map(convertStockFundType)
        localData.persist();
        println("6-------------localdataRdd最终要插入hbase的-count:-" + localData.count())
        // println(hiveContext.sql(selectSnapSql).head(1))
        if (insertData.equals("T")) {
          localData.saveAsNewAPIHadoopDataset(jobConf.getConfiguration)
        } else {
          val firstRec = midStockDF.head(1)(0)
          val format = new SimpleDateFormat("yyyyMMddHHmmssSSS")
          val secid = firstRec.getString(0)
          val MDDate = firstRec.getString(2).substring(0, 8);
          val origt = format.parse(firstRec.getString(2))
          val HTSCSecurityID = secid + ".SZ"
          val rowKey: String = MD5Hash.getMD5AsHex(HTSCSecurityID.getBytes()).substring(0, 6) + HTSCSecurityID + (java.lang.Long.MAX_VALUE - origt.getTime)
          println("第一个记录：time:" + firstRec.getString(2) + " sec:" + HTSCSecurityID + " rowKey:" + rowKey)
        }

        println("导入" + year + "/" + month + "/" + daye + "的snapshotlevel数据结束")

      }
    }

    //指定输出格式和输出表名
    //val jobConf2 = new JobConf(hbaseconf, this.getClass)
    // jobConf2.setOutputFormat(classOf[TableOutputFormat])
    // jobConf2.set(TableOutputFormat.OUTPUT_TABLE, "MDC:MDFundRecord")
    // val localData2 = hiveContext.sql(selectSnapSql).map(convertStockFundType(_,3))

    // localData2.saveAsHadoopDataset(jobConf2)
    sc.stop()
  }

  def insertSnapEveryDay(hiveContext: HiveContext, prdDF: DataFrame, year: String, mon: String, day: String, zkQurom: String): Unit = {

  }

  /**
    * Step 2：RDD 到表模式的映射
    */
  def convertStockFundType(row: Row): (ImmutableBytesWritable, Put) = {
    // val rb = ResourceBundle.getBundle("prdInfo".trim)
    //val keys=rb.getKeys
    val format = new SimpleDateFormat("yyyyMMddHHmmssSSS")
    val secid = row.getAs[String]("securityid")
    val mdstreamid = row.getAs[String]("mdstreamid")
    val origtime = row.getAs[String]("origtime")
    val MDDate = row.getAs[String]("tradedate");
    val origt = format.parse(origtime)
    val preclosepx = row.getAs[Double]("preclosepx")
    val pxchange1 = row.getAs[Double]("pxchange1")
    val pxchange2 = row.getAs[Double]("pxchange2")
    val openpx = row.getAs[Double]("openpx")
    val highpx = row.getAs[Double]("highpx")
    val lowpx = row.getAs[Double]("lowpx")
    val lastpx = row.getAs[Double]("lastpx")
    val numtrades = row.getAs[Long]("numtrades")
    val totalVolumnTrade = row.getAs[Long]("totalVolumnTrade")
    val totalValueTrade = row.getAs[Double]("totalValueTrade")
    val uplimitpx =row.getAs[Double]("uplimitpx")
    val downlimitpx = row.getAs[Double]("downlimitpx")
    var totalofferqty =row.getAs[String]("totalofferqty")
    var totalbidQty = row.getAs[String]("totalbidQty")
    if (totalofferqty.contains(".")) {
      totalofferqty = totalofferqty.split("\\.", 2)(0)
    }
    if (totalbidQty.contains(".")) {
      totalbidQty = totalbidQty.split("\\.", 2)(0)
    }

    val weightebidpx = row.getAs[Double]("weightedavgbidpx")
    val weiavgofferpx =row.getAs[Double]("weightedavgofferpx")
    val peratio1 = row.getAs[Double]("peratio1")
    val peratio2 = row.getAs[Double]("peratio2")
    val trphaseCode = row.getAs[String]("tradingphasecode").trim
    val warrantrate = row.getAs[Double]("warrantpremiumrate")
    val bidpx1 = row.getAs[Double]("bidpx1")
    val bidsize1 = row.getAs[Long]("bidsize1")
    val numorderb1 = row.getAs[Long]("numorders_b1")
    val noorderb1 = row.getAs[Long]("noorders_b1")
    val orderqtyb1 =row.getAs[String]("orderqty_b1")

    val offerpx1 = row.getAs[Double]("offerpx1")
    val offersize1 = row.getAs[Long]("offersize1")
    val numorders1 = row.getAs[Long]("numorders_s1")
    val noorders1 = row.getAs[Long]("noorders_s1")
    val orderqtys1 = row.getAs[String]("orderqty_s1")


    val offerpx2 = row.getAs[Double]("offerpx2")
    val offersize2 =  row.getAs[Long]("offersize2")
    val bidpx2 =  row.getAs[Double]("bidpx2")
    val bidsize2 =  row.getAs[Long]("bidsize2")
    val bidpx3 =  row.getAs[Double]("bidpx3")
    val bidsize3 =  row.getAs[Long]("bidsize3")
    val offerpx3 =  row.getAs[Double]("offerpx3")
    val offersize3 =  row.getAs[Long]("offersize3")
    val offerpx4 =  row.getAs[Double]("offerpx4")
    val offersize4 =  row.getAs[Long]("offersize4")
    val bidpx4 =  row.getAs[Double]("bidpx4")
    val bidsize4 =  row.getAs[Long]("bidsize4")
    val bidpx5 =  row.getAs[Double]("bidpx5")
    val bidsize5 =  row.getAs[Long]("bidsize5")
    val offerpx5 =  row.getAs[Double]("offerpx5")
    val offersize5 =  row.getAs[Long]("offersize5")
    val offerpx6 =  row.getAs[Double]("offerpx6")
    val offersize6 =  row.getAs[Long]("offersize6")
    val bidpx6 =  row.getAs[Double]("bidpx6")
    val bidsize6 =  row.getAs[Long]("bidsize6")
    val bidpx7 =  row.getAs[Double]("bidpx7")
    val bidsize7 =  row.getAs[Long]("bidsize7")
    val offerpx7 =  row.getAs[Double]("offerpx7")
    val offersize7 =  row.getAs[Long]("offersize7")
    val offerpx8 =  row.getAs[Double]("offerpx8")
    val offersize8 =  row.getAs[Long]("offersize8")
    val bidpx8 =  row.getAs[Double]("bidpx8")
    val bidsize8 =  row.getAs[Long]("bidsize8")
    val bidpx9 =  row.getAs[Double]("bidpx9")
    val bidsize9 =  row.getAs[Long]("bidsize9")
    val offerpx9 =  row.getAs[Double]("offerpx9")
    val offersize9 =  row.getAs[Long]("offersize9")
    val offerpx10 =  row.getAs[Double]("offerpx10")
    val offersize10 =  row.getAs[Long]("offersize10")
    val bidpx10 =  row.getAs[Double]("bidpx10")
    val bidsize10 =  row.getAs[Long]("bidsize10")

    println("-------------------77777------------------")
    var tpc: String =
      trphaseCode match {
        case "S0" => "0";
        case "O0" => "1";
        case "T0" => "3";
        case "C0" => "5";
        case "E0" => "6";
        case "A0" => "7";
        case "H0" => "8";
        case "V0" => "9";
        case "S1" => "8";
        case "O1" => "8";
        case "B1" => "8";
        case "T1" => "8";
        case "C1" => "8";
        case "E1" => "8";
        case "A1" => "8";
        case "H1" => "8";
        case "V1" => "8";
        case "S" => "0";
        case "O" => "1";
        case "T" => "3";
        case "C" => "5";
        case "E" => "6";
        case "A" => "7";
        case "H" => "8";
        case "V" => "9";
        case "B0" => "4";
      };
    val hm = row.getString(2).substring(8, 12)
    if ((hm >= "0925" && hm <= "0930")) {
      tpc = "2"
    }
    if ((hm >= "1130" && hm <= "1300")) {
      tpc = "4"
    }



    val HTSCSecurityID = secid+".SZ"
    //secid=secid.substring(0,secid.length-3)
    val rowKey: String = MD5Hash.getMD5AsHex(HTSCSecurityID.getBytes()).substring(0, 6) + HTSCSecurityID + (java.lang.Long.MAX_VALUE - origt.getTime)

    //val props = rb.getString(HTSCSecurityID)
    //val str = props.trim.split(",")
    val sectype = row.getAs[String]("sectype")
    val subType =row.getAs[String]("secsubtype")
    val symbol = row.getAs[String]("symbol")
    val p = new Put(Bytes.toBytes(rowKey))
    if(sectype.equals("3")){
      //基金类型
      val PreIOPV=row.getAs[Double]("prenav")
      val IOPV=row.getAs[String]("realtimenav")
      if(null!=IOPV && !IOPV.equals("NULL")){
        p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("IOPV"), Bytes.toBytes(IOPV))
      }
      if(null!=PreIOPV && !PreIOPV.equals("NULL")) {
        p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("PreIOPV"), Bytes.toBytes(PreIOPV.toString))
      }
    }
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("MDDate"), Bytes.toBytes(MDDate))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("MDTime"), Bytes.toBytes(origtime.substring(8)))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("ReceiveDateTime"), Bytes.toBytes(origtime))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("MDStreamID"), Bytes.toBytes(mdstreamid))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("SecurityType"), Bytes.toBytes(sectype))
    if ("-" != subType) {
      p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("SecuritySubType"), Bytes.toBytes(subType))
    }
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("SecurityID"), Bytes.toBytes(secid))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("SecurityIDSource"), Bytes.toBytes("102"))
    if ("-" != symbol) {
      p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("Symbol"), Bytes.toBytes(symbol))
    }
    //HTSCSecurityID
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("HTSCSecurityID"), Bytes.toBytes(HTSCSecurityID))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("MDLevel"), Bytes.toBytes("1"))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("MDChannel"), Bytes.toBytes("4"))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("MDRecordType"), Bytes.toBytes("1"))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("TradingPhaseCode"), Bytes.toBytes(tpc))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("PreClosePx"), Bytes.toBytes(preclosepx.toString))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("NumTrades"), Bytes.toBytes(numtrades.toString))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("TotalVolumeTrade"), Bytes.toBytes(totalVolumnTrade.toString))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("TotalValueTrade"), Bytes.toBytes(totalValueTrade.toString))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("LastPx"), Bytes.toBytes(lastpx.toString))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("OpenPx"), Bytes.toBytes(openpx.toString))
    // p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("ClosePx"), Bytes.toBytes())
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("HighPx"), Bytes.toBytes(highpx.toString))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("LowPx"), Bytes.toBytes(lowpx.toString))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("DiffPx1"), Bytes.toBytes(pxchange1.toString))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("DiffPx2"), Bytes.toBytes(pxchange2.toString))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("MaxPx"), Bytes.toBytes(uplimitpx.toString))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("MinPx"), Bytes.toBytes(downlimitpx.toString))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("TotalBidQty"), Bytes.toBytes(totalbidQty.toString))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("TotalOfferQty"), Bytes.toBytes(totalofferqty.toString))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("WeightedAvgBidPx"), Bytes.toBytes(weightebidpx.toString))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("WeightedAvgOfferPx"), Bytes.toBytes(weiavgofferpx.toString))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("SLYOne"), Bytes.toBytes(peratio1.toString))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("SLYTwo"), Bytes.toBytes(peratio2.toString))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("Buy1Price"), Bytes.toBytes(bidpx1.toString))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("Buy1OrderQty"), Bytes.toBytes(bidsize1.toString))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("Buy1NumOrders"), Bytes.toBytes(numorderb1.toString))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("Buy1NoOrders"), Bytes.toBytes(noorderb1.toString))

    if (StringUtils.isNotBlank(orderqtyb1)) {
      val aa: List[String] = orderqtyb1.trim.split("\\|").toList;
      val ld: java.util.List[Double] = new java.util.ArrayList[Double]
      for (a <- aa) {
        ld.add(java.lang.Double.parseDouble(a))
      }
      p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("Buy1OrderDetail"), Bytes.toBytes(ld.toString))

    }

    //p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("Buy1OrderDetail"), Bytes.toBytes(orderqtyb1))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("Sell1Price"), Bytes.toBytes(offerpx1.toString))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("Sell1OrderQty"), Bytes.toBytes(offersize1.toString))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("Sell1NumOrders"), Bytes.toBytes(numorders1.toString))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("Sell1NoOrders"), Bytes.toBytes(noorders1.toString))
    if (StringUtils.isNotBlank(orderqtys1)) {
      val aa: List[String] = orderqtys1.trim.split("\\|").toList;
      //val bb:List[String]=orderqtyb1.trim.split("\\|").toList;
      val ld: java.util.List[Double] = new java.util.ArrayList[Double]
      // val ld2: java.util.List[Double] = new java.util.ArrayList[Double]
      for (a <- aa) {
        ld.add(java.lang.Double.parseDouble(a))
      }
      p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("Sell1OrderDetail"), Bytes.toBytes(ld.toString)) //----
    }
    // p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("Sell1OrderDetail"), Bytes.toBytes(orderqtys1))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("Buy2Price"), Bytes.toBytes(bidpx2.toString))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("Buy2OrderQty"), Bytes.toBytes(bidsize2.toString))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("Sell2Price"), Bytes.toBytes(offerpx2.toString))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("Sell2OrderQty"), Bytes.toBytes(offersize2.toString))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("Buy3Price"), Bytes.toBytes(bidpx3.toString))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("Buy3OrderQty"), Bytes.toBytes(bidsize3.toString))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("Sell3Price"), Bytes.toBytes(offerpx3.toString))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("Sell3OrderQty"), Bytes.toBytes(offersize3.toString))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("Buy4Price"), Bytes.toBytes(bidpx4.toString))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("Buy4OrderQty"), Bytes.toBytes(bidsize4.toString))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("Sell4Price"), Bytes.toBytes(offerpx4.toString))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("Sell4OrderQty"), Bytes.toBytes(offersize4.toString))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("Buy5Price"), Bytes.toBytes(bidpx5.toString))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("Buy5OrderQty"), Bytes.toBytes(bidsize5.toString))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("Sell5Price"), Bytes.toBytes(offerpx5.toString))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("Sell5OrderQty"), Bytes.toBytes(offersize5.toString))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("Buy6Price"), Bytes.toBytes(bidpx6.toString))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("Buy6OrderQty"), Bytes.toBytes(bidsize6.toString))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("Sell6Price"), Bytes.toBytes(offerpx6.toString))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("Sell6OrderQty"), Bytes.toBytes(offersize6.toString))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("Buy7Price"), Bytes.toBytes(bidpx7.toString))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("Buy7OrderQty"), Bytes.toBytes(bidsize7.toString))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("Sell7Price"), Bytes.toBytes(offerpx7.toString))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("Sell7OrderQty"), Bytes.toBytes(offersize7.toString))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("Buy8Price"), Bytes.toBytes(bidpx8.toString))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("Buy8OrderQty"), Bytes.toBytes(bidsize8.toString))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("Sell8Price"), Bytes.toBytes(offerpx8.toString))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("Sell8OrderQty"), Bytes.toBytes(offersize8.toString))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("Buy9Price"), Bytes.toBytes(bidpx9.toString))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("Buy9OrderQty"), Bytes.toBytes(bidsize9.toString))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("Sell9Price"), Bytes.toBytes(offerpx9.toString))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("Sell9OrderQty"), Bytes.toBytes(offersize9.toString))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("Buy10Price"), Bytes.toBytes(bidpx10.toString))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("Buy10OrderQty"), Bytes.toBytes(bidsize10.toString))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("Sell10Price"), Bytes.toBytes(offerpx10.toString))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("Sell10OrderQty"), Bytes.toBytes(offersize10.toString))
    //  println("返回stocktype："+typek+"--"+sectype)
    (new ImmutableBytesWritable, p)

  }


  def insertIntoSnapRecord(): Unit = {

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
          val location = yearstr + monstr + daystr;
          days = location +: days
        }
      }
    }
    days
  }


}
