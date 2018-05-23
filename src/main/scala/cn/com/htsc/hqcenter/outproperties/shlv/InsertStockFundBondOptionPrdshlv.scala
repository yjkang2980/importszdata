package cn.com.htsc.hqcenter.outproperties.shlv

import java.text.SimpleDateFormat

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
  * 上交所历史文件导入，六个参数，第一个参数导入起始日期，第二个参数导入结束日期，第三个参数是否插入数据，第四个参数插入的表名称，第五个参数代表股票、基金，债券类表代码，第六个参数代表是否仅预处理T代表仅预处理
  * 参数示例：20140102 20140130 T MDC:MDStockRecord 2 T
  * MDC:MDFundRecord 基金
  * MDC:MDBondRecord 债券
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

object InsertStockFundBondOptionPrdshlv {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("hqshlv2snapdataimport")
    val sc = new SparkContext(conf)
    println("1-------------------------------------")
    val hiveContext = new HiveContext(sc)
    hiveContext.sql("use mdc")
    /*val prdinfoRdd = sc.textFile("hdfs://nameservice1/user/u010571/data/prdInfo.txt")

    val newprd = prdinfoRdd.map(row => {
      Row(row.split(",")(0), row.split(",")(1), row.split(",")(2), row.split(",")(3))
    }).filter(row => {
      row(0).toString.endsWith(".SH")
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
    prdDF.persist()
    prdDF.registerTempTable("prdDF")*/


    val hdfs = org.apache.hadoop.fs.FileSystem.get(new java.net.URI("hdfs://nameservice1"), new org.apache.hadoop.conf.Configuration())
    val origFiles:Array[FileStatus] = hdfs.listStatus(new Path("hdfs://nameservice1/user/u010571/History_data/his_sh2"))
    val snapfls: Array[FileStatus] = hdfs.listStatus(new Path("hdfs://nameservice1/user/u010571/shlv2/snapshot"))

    val days = getDayList(hdfs, snapfls)
    val startDay = args(0)
    val endDay = args(1)

    for (origf <- origFiles){
      val ymd=origf.getPath.toString.substring(origf.getPath.toString.length-8,origf.getPath.toString.length);
      if(ymd>=startDay && ymd<=endDay){
        val snapFile=new Path(origf.getPath.toString+"/Snapshot.csv");
        if(hdfs.exists(snapFile)) {
          // val tickFile=new Path(origf.getPath.toString+"/Auction.csv");
          val descSnap = new Path("hdfs://nameservice1/user/u010571/shlv2/snapshot/" + ymd + "/" + "Snapshot.csv");
          //val descTick=new Path("hdfs://nameservice1/user/u010571/shlv2/tradeticker/"+ymd+"/"+"Auction.csv");
          val pp = new Path("hdfs://nameservice1/user/u010571/shlv2/snapshot/" + ymd);
          if (!hdfs.exists(pp)) {
            hdfs.mkdirs(pp);
          }
          if (hdfs.exists(descSnap)) {
            hdfs.delete(descSnap, true)
          }
          //  if(hdfs.exists(descTick)){
          //    hdfs.delete(descTick,true)
          // }
          println(snapFile + " \n " + descSnap)
          hdfs.rename(snapFile, descSnap)
          // hdfs.rename(tickFile,descTick)
          val dropp = "ALTER TABLE table_app_t_md_snap_level_shlv_orig  DROP IF EXISTS PARTITION (ymd='" + ymd + "')"
          println(" 如果已有删除分区： " + ymd)
          hiveContext.sql(dropp)
          val strsql = "alter table table_app_t_md_snap_level_shlv_orig add partition (ymd='" + ymd + "') location '" + ymd + "'"
          println("分区语句：" + strsql)
          hiveContext.sql(strsql)
          println("已完成分区添加!" + "分区:" + ymd)
        }
      }
    }
    if(null!=args(5) && args(5).equals("T")){
      println("第六个参数为T，代表仅做数据预处理：移动文件到指定位置，建立hive外表分区映射")
      System.exit(1)
    }
   // val startTime = args(2)
   // val endTime = args(3)
    val insertData = args(2)
    for (day <- days) {
      if (day >= startDay && day <= endDay) {
        val year = day.substring(0, 4)
        val month = day.substring(4, 6)
        val daye = day.substring(6)
        println("开始导入" + year + "/" + month + "/" + daye + "的数据")
        val sstart = year + month + daye// + startTime
        val sendt = year + month + daye //+ endTime
        println("开始时间：" + sstart + " --结束时间：" + sendt + ",类别代码：" + args(4))
         //and a.origtime='20160831101024000' and a.securityid='300448'

        sc.hadoopConfiguration.set("hbase.zookeeper.quorum", "arch-bd-zookeeper2,arch-bd-zookeeper4,arch-bd-zookeeper1,arch-bd-zookeeper3,arch-bd-zookeeper5")
        sc.hadoopConfiguration.set("hbase.zookeeper.property.clientPort", "2181")
        sc.hadoopConfiguration.set(TableOutputFormat.OUTPUT_TABLE, args(3))
        //定义 HBase 的配置

        val jobConf = new Job(sc.hadoopConfiguration)
        jobConf.setOutputKeyClass(classOf[ImmutableBytesWritable])
        jobConf.setOutputValueClass(classOf[Result])
        jobConf.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])

        //val selectSnapSql = "select *from table_app_t_md_snap_level_shlv_orig t,mdc_center_prdt_info p where t.ymd='"+day+"' and p.sectype='"+args(4)+"' and p.securityid=concat(t.securityid,'.SH') and instr(p.securityid,'SH')>0  and  t.securityid!='HEAD:v1' and t.securityid='601336' and t.datetime='20140102133321' order by t.datetime asc,t.securityid asc,cast(t.totalvolumetrade as bigint)  asc  "
        val selectSnapSql = "select *from table_app_t_md_snap_level_shlv_orig t,mdc_center_prdt_info p where t.ymd='"+day+"' and p.sectype='"+args(4)+"' and p.securityid=concat(t.securityid,'.SH') and instr(p.securityid,'SH')>0  and  t.securityid!='HEAD:v1' order by t.datetime asc,t.securityid asc,cast(t.totalvolumetrade as bigint)  asc  "

        println("4-------------------------------------")
        println("sql:" + selectSnapSql)

        val ldDF = hiveContext.sql(selectSnapSql)
       // println("5-----------------------count:" + ldDF.count())
        var midStockDF = ldDF//.join(prdDF, ldDF("securityid") === prdDF("prdtid")).filter("sectype='" + args(4) + "'")
        //midStockDF.persist()
        //midStockDF=midStockDF.filter(midStockDF("securityid").equalTo("601336")).filter(midStockDF("datetime").equalTo("20140102133321"));
        //  val localData = hiveContext.sql(selectSnapSql).map(convertStockFundType(_,2))
        val localData = midStockDF.map(convertStockFundType)

        // println(hiveContext.sql(selectSnapSql).head(1))
        //localData.persist()
        val c1= localData.count()
        if (insertData.equals("T")) {
          println("6-------------------------共插入"+day+"--" +c1+"条snap数据")
          localData.saveAsNewAPIHadoopDataset(jobConf.getConfiguration)
        } else {
          val firstRec = midStockDF.head(1)(0)
          val format = new SimpleDateFormat("yyyyMMddHHmmssSS")
          val secid = firstRec.getAs[String]("securityid")
          val MDDate =  firstRec.getAs[String]("tradetime").substring(0, 8);
          val origt = format.parse(firstRec.getAs[String]("tradetime"))
          val HTSCSecurityID = secid + ".SH"
          val rowKey: String = MD5Hash.getMD5AsHex(HTSCSecurityID.getBytes()).substring(0, 6) + HTSCSecurityID + (java.lang.Long.MAX_VALUE - origt.getTime)
          println("第一个记录：time:" + firstRec.getAs[String]("tradetime") + " sec:" + HTSCSecurityID + " rowKey:" + rowKey)
        }

        println("导入" + year + "/" + month + "/" + daye + "的snapshotlevel数据结束:"+c1+"条,表："+ args(3))

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
    val format = new SimpleDateFormat("yyyyMMddHHmmss")

    var secid = row.getAs[String]("securityid")

    val origtime = row.getAs[String]("datetime")
    val MDDate = row.getAs[String]("datetime").substring(0,8);
    val origt = format.parse(origtime)
    val preclosepx = row.getAs[String]("preclosepx")

    val openpx = row.getAs[String]("openpx")
    val highpx = row.getAs[String]("highpx")
    val lowpx = row.getAs[String]("lowpx")
    val lastpx = row.getAs[String]("lastpx")

    val totalVolumnTrade = row.getAs[String]("totalvolumetrade")
    val totalValueTrade = row.getAs[String]("totalvaluetrade")

    var totalofferqty =row.getAs[String]("totalofferqty")
    var totalbidQty = row.getAs[String]("totalbidqty")
    var totalbidnumber =row.getAs[String]("totalbidnumber")
    var totaloffernumber = row.getAs[String]("totaloffernumber")
    val bidtrademaxduration=row.getAs[String]("bidtrademaxduration")
    val offertrademaxduration=row.getAs[String]("offertrademaxduration")
    val numbidorders=row.getAs[String]("numbidorders")
    val numofferorders=row.getAs[String]("numofferorders")
    val withdrawbuynumber=row.getAs[String]("withdrawbuynumber")
    val withdrawbuyamount=row.getAs[String]("withdrawbuyamount")
    val withdrawbuymoney=row.getAs[String]("withdrawbuymoney")
    val withdrawsellnumber=row.getAs[String]("withdrawsellnumber")
    val withdrawsellamount=row.getAs[String]("withdrawsellamount")
    val withdrawsellmoney=row.getAs[String]("withdrawsellmoney")

    val weightebidpx = row.getAs[String]("weightedavgbidpx")
    val weiavgofferpx =row.getAs[String]("weightedavgofferpx")
    val numtrades = row.getAs[String]("numtrades")

    val trphaseCode = row.getAs[String]("instrumentstatus").trim
    //val warrantrate = row.getAs[Double]("warrantpremiumrate")

    val bidpx1 = row.getAs[String]("bidpx1")
    val bidsize1 = row.getAs[String]("bid1orderqty")
    val numorderb1 = row.getAs[String]("bid1numorders")
    //val noorderb1 = row.getAs[String]("noorders_b1")
    //买1委托队列
    val ldbid1: java.util.List[Double] = new java.util.ArrayList[Double]
    for(i <- 1 to 50){
      val noorderbi = row.getAs[String]("bid1orders"+i)
      try {
        val noorders=java.lang.Double.parseDouble(noorderbi);
        if(noorders!=0.0){
           ldbid1.add(noorders);
        }
      }catch {
        case e:Exception=>e.printStackTrace()
      }
    }
    val noorderb1 = ldbid1.size()

    val offerpx1 = row.getAs[String]("offer1price")
    val offersize1 = row.getAs[String]("offer1orderqty")
    val numorders1 = row.getAs[String]("offer1numorders")

    val orderqtys1: java.util.List[Double] = new java.util.ArrayList[Double]
    for(i <- 1 to 50){
      val noordersi = row.getAs[String]("offer1orders"+i)
      try {
        val noorders=java.lang.Double.parseDouble(noordersi);
        if(noorders!=0.0){
          orderqtys1.add(noorders);
        }
      }catch {
        case e:Exception=>e.printStackTrace()
      }
    }
    val noorders1 = orderqtys1.size()

    val offerpx2 = row.getAs[String]("offer2price")
    val offersize2 = row.getAs[String]("offer2orderqty")
    val numorders2 = row.getAs[String]("offer2numorders")
    val bidpx2 = row.getAs[String]("bidpx2")
    val bidsize2 = row.getAs[String]("bid2orderqty")
    val numorderb2 = row.getAs[String]("bid2numorders")

    val offerpx3 = row.getAs[String]("offer3price")
    val offersize3 = row.getAs[String]("offer3orderqty")
    val numorders3 = row.getAs[String]("offer3numorders")
    val bidpx3 = row.getAs[String]("bidpx3")
    val bidsize3 = row.getAs[String]("bid3orderqty")
    val numorderb3 = row.getAs[String]("bid3numorders")

    val offerpx4 = row.getAs[String]("offer4price")
    val offersize4 = row.getAs[String]("offer4orderqty")
    val numorders4 = row.getAs[String]("offer4numorders")
    val bidpx4 = row.getAs[String]("bidpx4")
    val bidsize4 = row.getAs[String]("bid4orderqty")
    val numorderb4 = row.getAs[String]("bid4numorders")

    val offerpx5 = row.getAs[String]("offer5price")
    val offersize5 = row.getAs[String]("offer5orderqty")
    val numorders5 = row.getAs[String]("offer5numorders")
    val bidpx5 = row.getAs[String]("bidpx5")
    val bidsize5 = row.getAs[String]("bid5orderqty")
    val numorderb5 = row.getAs[String]("bid5numorders")

    val offerpx6 = row.getAs[String]("offer6price")
    val offersize6 = row.getAs[String]("offer6orderqty")
    val numorders6 = row.getAs[String]("offer6numorders")
    val bidpx6 = row.getAs[String]("bidpx6")
    val bidsize6 = row.getAs[String]("bid6orderqty")
    val numorderb6 = row.getAs[String]("bid6numorders")

    val offerpx7 = row.getAs[String]("offer7price")
    val offersize7 = row.getAs[String]("offer7orderqty")
    val numorders7 = row.getAs[String]("offer7numorders")
    val bidpx7 = row.getAs[String]("bidpx7")
    val bidsize7 = row.getAs[String]("bid7orderqty")
    val numorderb7 = row.getAs[String]("bid7numorders")

    val offerpx8 = row.getAs[String]("offer8price")
    val offersize8 = row.getAs[String]("offer8orderqty")
    val numorders8 = row.getAs[String]("offer8numorders")
    val bidpx8 = row.getAs[String]("bidpx8")
    val bidsize8 = row.getAs[String]("bid8orderqty")
    val numorderb8 = row.getAs[String]("bid8numorders")

    val offerpx9 = row.getAs[String]("offer9price")
    val offersize9 = row.getAs[String]("offer9orderqty")
    val numorders9 = row.getAs[String]("offer9numorders")
    val bidpx9 = row.getAs[String]("bidpx9")
    val bidsize9 = row.getAs[String]("bid9orderqty")
    val numorderb9 = row.getAs[String]("bid9numorders")

    val offerpx10 = row.getAs[String]("offer10price")
    val offersize10 = row.getAs[String]("offer10orderqty")
    val numorders10 = row.getAs[String]("offer10numorders")
    val bidpx10 = row.getAs[String]("bidpx10")
    val bidsize10 = row.getAs[String]("bid10orderqty")
    val numorderb10 = row.getAs[String]("bid10numorders")

    val sectype = row.getAs[String]("sectype")
    val subType =row.getAs[String]("secsubtype")
    val symbol = row.getAs[String]("symbol")

    println("-------------------77777------------------")
    var tpc: String =
      trphaseCode match {
        case "BETW" => "2";
        case "START" => "0";
        case "OCALL" => "1";
        case "ICALL" => "1";
        case "OOBB" => "2";
        case "OPOBB" => "2";
        case "TRADE" => "3";
        case "BREAK" => "4";
        case "CLOSE" => "6";
        case "SUSP" => "2";
        case "SUSPE" => "8";
        case "HALT" => "8";
        case "ENDTR" => "7";
        case "VOLA" => "9";
      };

    var HTSCSecurityID = ""
    if(secid.length==6){
      secid + ".SZ"
    }else{
      HTSCSecurityID=secid
    }

    val rowKey: String = MD5Hash.getMD5AsHex(HTSCSecurityID.getBytes()).substring(0, 6) + HTSCSecurityID + (java.lang.Long.MAX_VALUE - origt.getTime)

    //val props = rb.getString(HTSCSecurityID)
    //val str = props.trim.split(",")

    val p = new Put(Bytes.toBytes(rowKey))

    if(sectype.equals("3")){
     //基金类型
      val iopv=row.getAs[String]("iopv")
      p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("IOPV"), Bytes.toBytes(iopv))
    }

    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("MDDate"), Bytes.toBytes(MDDate))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("MDTime"), Bytes.toBytes(origtime))
   // p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("MDStreamID"), Bytes.toBytes(mdstreamid))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("SecurityType"), Bytes.toBytes(sectype))
    if ("-" != subType) {
      p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("SecuritySubType"), Bytes.toBytes(subType))
    }
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("SecurityID"), Bytes.toBytes(secid))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("SecurityIDSource"), Bytes.toBytes("101"))
    if ("-" != symbol) {
      p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("Symbol"), Bytes.toBytes(symbol))
    }
    //HTSCSecurityID
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("HTSCSecurityID"), Bytes.toBytes(HTSCSecurityID))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("MDLevel"), Bytes.toBytes("1"))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("MDChannel"), Bytes.toBytes("3"))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("MDRecordType"), Bytes.toBytes("1"))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("TradingPhaseCode"), Bytes.toBytes(tpc))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("PreClosePx"), Bytes.toBytes(preclosepx))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("NumTrades"), Bytes.toBytes(numtrades))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("TotalVolumeTrade"), Bytes.toBytes(totalVolumnTrade))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("TotalValueTrade"), Bytes.toBytes(totalValueTrade))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("LastPx"), Bytes.toBytes(lastpx))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("OpenPx"), Bytes.toBytes(openpx))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("PreClosePx"), Bytes.toBytes(preclosepx))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("HighPx"), Bytes.toBytes(highpx))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("LowPx"), Bytes.toBytes(lowpx))

    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("TotalBidQty"), Bytes.toBytes(totalbidQty))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("TotalOfferQty"), Bytes.toBytes(totalofferqty))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("WeightedAvgBidPx"), Bytes.toBytes(weightebidpx))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("WeightedAvgOfferPx"), Bytes.toBytes(weiavgofferpx))

    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("TotalBidQty"), Bytes.toBytes(totalbidQty))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("TotalOfferQty"), Bytes.toBytes(totalofferqty))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("WithdrawBuyNumber"), Bytes.toBytes(withdrawbuynumber))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("WithdrawBuyAmount"), Bytes.toBytes(withdrawbuyamount))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("WithdrawBuyMoney"), Bytes.toBytes(withdrawbuymoney))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("WithdrawSellNumber"), Bytes.toBytes(withdrawsellnumber))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("WithdrawSellAmount"), Bytes.toBytes(withdrawsellamount))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("WithdrawSellMoney"), Bytes.toBytes(withdrawsellmoney))

    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("TotalBidNumber"), Bytes.toBytes(totalbidnumber))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("TotalOfferNumber"), Bytes.toBytes(totaloffernumber))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("BidTradeMaxDuration"), Bytes.toBytes(bidtrademaxduration))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("OfferTradeMaxDuration"), Bytes.toBytes(offertrademaxduration))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("NumBidOrders"), Bytes.toBytes(numbidorders))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("NumOfferOrders"), Bytes.toBytes(numofferorders))

    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("Buy1Price"), Bytes.toBytes(bidpx1))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("Buy1OrderQty"), Bytes.toBytes(bidsize1))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("Buy1NumOrders"), Bytes.toBytes(numorderb1))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("Buy1NoOrders"), Bytes.toBytes(noorderb1.toString))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("Buy1OrderDetail"), Bytes.toBytes(ldbid1.toString))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("Sell1Price"), Bytes.toBytes(offerpx1))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("Sell1OrderQty"), Bytes.toBytes(offersize1))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("Sell1NumOrders"), Bytes.toBytes(numorders1))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("Sell1NoOrders"), Bytes.toBytes(noorders1.toString))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("Sell1OrderDetail"), Bytes.toBytes(orderqtys1.toString))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("Buy2Price"), Bytes.toBytes(bidpx2))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("Buy2NumOrders"), Bytes.toBytes(numorderb2))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("Buy2OrderQty"), Bytes.toBytes(bidsize2))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("Sell2Price"), Bytes.toBytes(offerpx2))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("Sell2NumOrders"), Bytes.toBytes(numorders2))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("Sell2OrderQty"), Bytes.toBytes(offersize2))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("Buy3Price"), Bytes.toBytes(bidpx3))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("Buy3NumOrders"), Bytes.toBytes(numorderb3))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("Buy3OrderQty"), Bytes.toBytes(bidsize3))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("Sell3Price"), Bytes.toBytes(offerpx3))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("Sell3NumOrders"), Bytes.toBytes(numorders3))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("Sell3OrderQty"), Bytes.toBytes(offersize3))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("Buy4Price"), Bytes.toBytes(bidpx4))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("Buy4NumOrders"), Bytes.toBytes(numorderb4))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("Buy4OrderQty"), Bytes.toBytes(bidsize4))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("Sell4Price"), Bytes.toBytes(offerpx4))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("Sell4NumOrders"), Bytes.toBytes(numorders4))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("Sell4OrderQty"), Bytes.toBytes(offersize4))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("Buy5Price"), Bytes.toBytes(bidpx5))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("Buy5NumOrders"), Bytes.toBytes(numorderb5))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("Buy5OrderQty"), Bytes.toBytes(bidsize5))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("Sell5Price"), Bytes.toBytes(offerpx5))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("Sell5NumOrders"), Bytes.toBytes(numorders5))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("Sell5OrderQty"), Bytes.toBytes(offersize5))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("Buy6Price"), Bytes.toBytes(bidpx6))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("Buy6NumOrders"), Bytes.toBytes(numorderb6))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("Buy6OrderQty"), Bytes.toBytes(bidsize6))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("Buy7Price"), Bytes.toBytes(bidpx7))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("Buy7NumOrders"), Bytes.toBytes(numorderb7))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("Buy7OrderQty"), Bytes.toBytes(bidsize7))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("Sell7Price"), Bytes.toBytes(offerpx7))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("Sell7NumOrders"), Bytes.toBytes(numorders7))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("Sell7OrderQty"), Bytes.toBytes(offersize7))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("Buy8Price"), Bytes.toBytes(bidpx8))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("Buy8NumOrders"), Bytes.toBytes(numorderb8))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("Buy8OrderQty"), Bytes.toBytes(bidsize8))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("Sell8Price"), Bytes.toBytes(offerpx8))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("Sell8NumOrders"), Bytes.toBytes(numorders8))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("Sell8OrderQty"), Bytes.toBytes(offersize8))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("Buy9Price"), Bytes.toBytes(bidpx9))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("Buy9NumOrders"), Bytes.toBytes(numorderb9))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("Buy9OrderQty"), Bytes.toBytes(bidsize9))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("Sell9Price"), Bytes.toBytes(offerpx9))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("Sell9NumOrders"), Bytes.toBytes(numorders9))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("Sell9OrderQty"), Bytes.toBytes(offersize9))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("Buy10Price"), Bytes.toBytes(bidpx10))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("Buy10NumOrders"), Bytes.toBytes(numorderb10))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("Buy10OrderQty"), Bytes.toBytes(bidsize10))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("Sell10Price"), Bytes.toBytes(offerpx10))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("Sell10NumOrders"), Bytes.toBytes(numorders10))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("Sell10OrderQty"), Bytes.toBytes(offersize10))
    //  println("返回stocktype："+typek+"--"+sectype)
    (new ImmutableBytesWritable, p)
  }


  def insertIntoSnapRecord(): Unit = {

  }


  def getNotExistsSecurityIds(sc: SparkContext, hiveContext: HiveContext, year: String, mon: String, day: String): List[String] = {


    val snapfls = "hdfs://nameservice1/user/u010571/data/prdInfo.properties"
    var m: Map[String, String] = Map()
    val mapRdd = sc.textFile(snapfls).map(row => Row(row.split("=")(0), row.split("=")(1)))
    mapRdd.foreach(
      row => {
        println(row)
        m += (row.getString(0) -> row.getString(1))
      }
    )

    val numsecId = "select a.securityid from table_app_t_md_snap a where a.year='" + year + "' and month='" + mon + "' and day='" + day + "'"

    val secids = hiveContext.sql(numsecId).distinct()
    var list: List[String] = List()
    secids.foreach(row => {
      val str = row.getString(0)
      list = str :: list
    })
    return list

  }


  /**
    * 校验每一天的数据是否符合逻辑
    *
    * @param hdfs
    * @param p1 example: val snapfls: Array[FileStatus] = hdfs.listStatus(new Path("hdfs://nameservice1/user/u010571/snapshotnew"))
    */
  def getDayList( hdfs: FileSystem, p1: Array[FileStatus]): List[String] = {
    var days: List[String] = List()
    for (fs <- p1) {
      val ymd = fs.getPath.toString;
      val yearstr = ymd.substring(ymd.length - 8, ymd.length)
      //val snapshotf=
      days = yearstr +: days
    }
    days
  }

  def moveFileToLoc(  hdfs: FileSystem, p1: Array[FileStatus],origFiles:Path): Unit ={

  }

}
