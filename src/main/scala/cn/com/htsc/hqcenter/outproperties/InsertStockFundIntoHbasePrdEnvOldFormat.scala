package cn.com.htsc.hqcenter.outproperties

import java.text.SimpleDateFormat
import java.util.ResourceBundle

import io.netty.util.internal.StringUtil
import org.apache.commons.lang.StringUtils
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.apache.hadoop.hbase.client.{Put, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.hbase.util.{Bytes, MD5Hash}
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
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

object InsertStockFundIntoHbasePrdEnvOldFormat {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("hqdataimport")
    val sc = new SparkContext(conf)
    println("1-------------------------------------")
    val hiveContext = new HiveContext(sc)
    hiveContext.sql("use mdc")
    val sqlContext = new SQLContext(sc)
    val rb = ResourceBundle.getBundle("prdInfo".trim)
    val keys = rb.getKeys
    val rb1 = ResourceBundle.getBundle("option")
    val prdinfoRdd = sc.textFile("hdfs://nameservice1/user/u010571/data/prdInfo.txt")
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
    var confh=new org.apache.hadoop.conf.Configuration();
    confh.set("dfs.replication","1")

    val hdfs = org.apache.hadoop.fs.FileSystem.get(new java.net.URI("hdfs://nameservice1"), confh)
    val snapfls: Array[FileStatus] = hdfs.listStatus(new Path("hdfs://nameservice1/user/u010571/mddata/snapshotold"))
    val days = getDayList(hiveContext, hdfs, snapfls)
    val startDay = args(0)
    val endDay = args(1)
    // val startTime=args(2)
    // val endTime=args(3)
    for (day <- days) {
      if (day >= startDay && day <= endDay) {
        val year = day.substring(0, 4)
        val month = day.substring(4, 6)
        val daye = day.substring(6)
        println("开始导入" + year + "/" + month + "/" + daye + "的数据")
        //  val sstart=year+month+daye+startTime
        //  val sendt=year+month+daye+endTime
        //  println("开始时间："+sstart+" --结束时间："+sendt+" 表名称："+args(4)+",类别代码："+args(5))
        //  val selectSnapSql="select *from table_app_t_md_snap_old t,table_app_t_md_snap_level_old q " +
        //    "where t.year='"+year+"' and t.month='"+month+"' and t.day='"+daye+"'  and t.year=q.year and t.month=q.month and t.day=q.day " +
        //   "and cast(t.datatimestamp as bigint)=q.datatimestamp"
        //and a.origtime='20160831101024000' and a.securityid='300448'

        sc.hadoopConfiguration.set("hbase.zookeeper.quorum", "arch-bd-zookeeper2,arch-bd-zookeeper4,arch-bd-zookeeper1,arch-bd-zookeeper3,arch-bd-zookeeper5")
        sc.hadoopConfiguration.set("hbase.zookeeper.property.clientPort", "2181")
        sc.hadoopConfiguration.set(TableOutputFormat.OUTPUT_TABLE, args(3))
        //定义 HBase 的配置
        //val hbaseconf = HBaseConfiguration.create()
        //hbaseconf.set("hbase.zookeeper.property.clientPort", "2181")
        //hbaseconf.set("hbase.zookeeper.quorum", zkQurom)
        val jobConf = new Job(sc.hadoopConfiguration)
        jobConf.setOutputKeyClass(classOf[ImmutableBytesWritable])
        jobConf.setOutputValueClass(classOf[Result])
        jobConf.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])

        //   println("4-------------------------------------")
        //  println("sql:"+selectSnapSql)

        // val ldDF=hiveContext.sql(selectSnapSql)
        // println("5-----------------------count:"+ldDF.count())

        //快照数据价格不合理,numtrades,totalvolumetrade,totalvaluetrade不合理记录数
        //val badRecs=getPriceSnapBadRecsOld(hiveContext,year,month,daye)


        //下面这个sql排除了很多异常情况，如价格异常，成交量没有递增等
        val sql = "SELECT t.* from table_app_t_md_snap_old t left outer join " +
          "(select distinct AA.securityid,AA.datatimestamp from(select t.securityid,t.numtrades," +
          "      lag(t.numtrades,1) over(partition by t.securityid order by t.datatimestamp asc,t.numtrades asc) as lastnumt," +
          "        t.totalvaluetrade,       lag(t.totalvaluetrade,1) over (partition by t.securityid order " +
          "by t.datatimestamp asc,t.numtrades asc) as lastval," +
          "      t.totalvolumetrade,lag(t.totalvolumetrade,1) over(partition by t.securityid order by t.datatimestamp asc,t.numtrades " +
          " asc) as lastvol," +
          "       t.datatimestamp,lag(t.datatimestamp,1) over(partition by t.securityid order by t.datatimestamp asc) as lasttime" +
          "  from table_app_t_md_snap_old t where t.year='" + year + "' and t.month='" + month + "' and t.day='" + daye + "' ) AA where (AA.numtrades<AA.lastnumt or " +
          "        AA.totalvaluetrade<AA.lastval or AA.totalvolumetrade<AA.lastvol)" +
          "       and (AA.datatimestamp between '93000000' and '113000000' or AA.datatimestamp between '130000000' and '145700000')" +
          " ) BB on t.securityid=BB.securityid and t.datatimestamp=BB.datatimestamp " +
          "WHERE t.year='" + year + "' and t.month='" + month + "' and t.day='" + daye + "'  and cast(t.preclosepx AS double)>=0" +
          "        AND cast(t.preclosepx AS double)<=99999999 AND cast(t.lastpx AS double)>=0  AND cast(t.lastpx AS double)<=99999999" +
          "     AND cast(t.openpx AS double)>=0   AND cast(t.openpx AS double)<=99999999  AND cast(t.highpx AS double)>=0" +
          " AND cast(t.highpx AS double)<=99999999   AND cast(t.lowpx AS double)>=0  AND cast(t.lowpx AS double)<=99999999  " +
          " AND cast(t.highpx AS double) >= cast(t.lastpx AS double)   AND cast(t.lowpx AS double)<=cast(t.lastpx AS double)    " +
          "AND t.numtrades>=0  AND t.totalvolumetrade>=0" +
          " AND t.totalvaluetrade>=0 and BB.securityid is null and BB.datatimestamp is null"
        var snapData = hiveContext.sql(sql)
        // snapData=snapData.dropDuplicates(Seq("securityid","datatimestamp"))
        println("snap快照数据条数：" + snapData.count())

        var snapLevelData = hiveContext.sql("select t.* from table_app_t_md_snap_level_old t left join " +
          "(select distinct t.securityid,t.datatimestamp from table_app_t_md_snap_level_old t where  " +
          "t.year='" + year + "' and t.month='" + month + "' and t.day='" + daye + "'  and  " +
          " ((t.bidpx1!=0.0 and t.bidsize1==0) or (t.bidpx2!=0.0 and t.bidsize2==0) or " +
          "(t.bidpx3!=0.0 and t.bidsize3==0) or (t.bidpx4!=0.0 and t.bidsize4==0) or  " +
          "(t.bidpx5!=0.0 and t.bidsize5==0) or (t.bidpx6!=0.0 and t.bidsize6==0) or " +
          "  (t.bidpx7!=0.0 and t.bidsize7==0) or (t.bidpx8!=0.0 and t.bidsize8==0) or " +
          "    (t.bidpx9!=0.0 and t.bidsize9==0) or (t.bidpx10!=0.0 and t.bidsize10==0) or " +
          " (t.offerpx1!=0.0 and t.offersize1==0) or (t.offerpx2!=0.0 and t.offersize2==0) or " +
          "  (t.offerpx3!=0.0 and t.offersize3==0) or (t.offerpx4!=0.0 and t.offersize4==0) or " +
          "  (t.offerpx5!=0.0 and t.offersize5==0) or (t.offerpx6!=0.0 and t.offersize6==0) or " +
          "  (t.offerpx7!=0.0 and t.offersize7==0) or (t.offerpx8!=0.0 and t.offersize8==0) or   " +
          "  (t.offerpx9!=0.0 and t.offersize9==0) or (t.offerpx10!=0.0 and t.offersize10==0) " +
          " ))AA on t.securityid=AA.securityid and t.datatimestamp=AA.datatimestamp " +
          "where t.year='" + year + "' and t.month='" + month + "' and t.day='" + daye + "'")

       // println("去掉十档行情价格不合理的记录数：" + snapLevelData.count()) // 需要去重

        snapData = snapData.orderBy(snapData("datatimestamp").asc).orderBy(snapData("totalvolumetrade").desc).dropDuplicates(Seq("securityid", "datatimestamp"))
        snapLevelData = snapLevelData.orderBy(snapLevelData("datatimestamp").asc).orderBy(snapLevelData("sendtime").desc).dropDuplicates(Seq("securityid", "datatimestamp"))


        val snapsnaplevel = snapData.join(snapLevelData, Seq("securityid", "datatimestamp"), "inner")
        println("十档行情和快照join后记录数：" + snapsnaplevel.count())
        // snapsnaplevel.orderBy(snapsnaplevel("datatimestamp").asc).orderBy(snapsnaplevel("totalvolumetrade").desc).dropDuplicates(Seq("securityid","datatimestamp")).count

        // val df22 = snapLevelData.withColumnRenamed("securityid","secid").withColumnRenamed("datatimestamp","dts")
        // val snapsnaplevel=snapData.join(df22,snapData("securityid")===df22("secid") && snapData("datatimestamp")===df22("dts"),"inner")

        val midStockDF = snapsnaplevel.join(prdDF, snapsnaplevel("securityid") === prdDF("prdtid")).filter("sectype='" + args(2) + "'")
        //  val localData = hiveContext.sql(selectSnapSql).map(convertStockFundType(_,2))
        val localData = midStockDF.map(convertStockFundType)
        if (args(4).toString.equals("T")) {
          localData.saveAsNewAPIHadoopDataset(jobConf.getConfiguration)
        } else {
          println("6-------------------------snaplevelcount:-" + snapsnaplevel.count() + " localData:" + localData.count())
          val firstRec = midStockDF.head(1)(0)
          val format = new SimpleDateFormat("yyyyMMddHHmmssSSS")
          val secid = firstRec.getAs[String]("securityid")
          val HTSCSecurityID = secid + ".SZ"
          var origtime = firstRec.getAs[Long]("datatimestamp").toString;
          if (origtime.length == 8) {
            origtime = "0" + origtime
          }
          origtime = firstRec.getAs[String]("tradedate") + origtime
          val origt = format.parse(origtime)

          val rowKey: String = MD5Hash.getMD5AsHex(HTSCSecurityID.getBytes()).substring(0, 6) + HTSCSecurityID + (java.lang.Long.MAX_VALUE - origt.getTime)

          println("第一个记录：time:" + origtime + " sec:" + HTSCSecurityID + " rowKey:" + rowKey)

        }
        // println(hiveContext.sql(selectSnapSql).head(1))
        println("导入" + year + "/" + month + "/" + daye + "的快照和十档行情数据结束")

      }
    }

    sc.stop()
  }

  /**
    * Step 2：RDD 到表模式的映射
    */
  def convertStockFundType(row: Row): (ImmutableBytesWritable, Put) = {
    // val rb = ResourceBundle.getBundle("prdInfo".trim)
    //val keys=rb.getKeys
    val format = new SimpleDateFormat("yyyyMMddHHmmssSSS")
    val secid = row.getAs[String]("securityid")
    //val mdstreamid=row.getString(1);
    var origtime = row.getAs[Long]("datatimestamp").toString;
    if (origtime.length == 8) {
      origtime = "0" + origtime
    }
    val MDDate = row.getAs[String]("tradedate");
    val origt = format.parse(MDDate + origtime)
    val preclosepx = row.getAs[Double]("preclosepx");
    /*val pxchange1=row.getDouble(4);val pxchange2=row.getDouble(5);*/
    val openpx = row.getAs[Double]("openpx");
    val highpx = row.getAs[Double]("highpx");
    val lowpx = row.getAs[Double]("lowpx");
    val lastpx = row.getAs[Double]("lastpx");
    val numtrades = row.getAs[Long]("numtrades");
    val totalVolumnTrade = row.getAs[Long]("totalvolumetrade");
    val totalValueTrade = row.getAs[Double]("totalvaluetrade");
    //val uplimitpx=row.getDouble(13);val downlimitpx=row.getDouble(14);
    var totalofferqty = row.getAs[Long]("totalofferqty");
    var totalbidQty = row.getAs[Long]("totalbidqty");
    //if(totalofferqty.contains(".")){
    //  totalofferqty=totalofferqty.split("\\.",2)(0)
    // }
    //if(totalbidQty.contains(".")){
    // totalbidQty=totalbidQty.split("\\.",2)(0)
    // }

    val weightebidpx = row.getAs[Double]("weightedavgbidpx");
    val weiavgofferpx = row.getAs[Double]("weightedavgofferpx");
    val peratio1 = row.getAs[Double]("peratio1");
    val peratio2 = row.getAs[Double]("peratio2");
    /*val trphaseCode=row.getString(21).trim;*/
    //  val warrantrate=row.getDouble(22);
    val bidpx1 = row.getAs[Double]("bidpx1");
    val bidsize1 = row.getAs[Long]("bidsize1");
    ;
    val numorderb1 = row.getAs[Long]("numorders_b1");
    val noorderb1 = row.getAs[Long]("noorders_b1");
    val orderqtyb1 = row.getAs[String]("orderqty_b1");
    val offerpx1 = row.getAs[Double]("offerpx1");
    val offersize1 = row.getAs[Long]("offersize1");
    val numorders1 = row.getAs[Long]("numorders_s1");
    val noorders1 = row.getAs[Long]("noorders_s1");
    val orderqtys1 = row.getAs[String]("orderqty_s1");
    val offerpx2 = row.getAs[Double]("offerpx2");
    val offersize2 = row.getAs[Long]("offersize2");
    val bidpx2 = row.getAs[Double]("bidpx2");
    val bidsize2 = row.getAs[Long]("bidsize2");
    val bidpx3 = row.getAs[Double]("bidpx3");
    val bidsize3 = row.getAs[Long]("bidsize3");
    val offerpx3 = row.getAs[Double]("offerpx3");
    val offersize3 = row.getAs[Long]("offersize3");
    val offerpx4 = row.getAs[Double]("offerpx4");
    val offersize4 = row.getAs[Long]("offersize4");
    val bidpx4 = row.getAs[Double]("bidpx4");
    val bidsize4 = row.getAs[Long]("bidsize4");
    ;
    val bidpx5 = row.getAs[Double]("bidpx5");
    val bidsize5 = row.getAs[Long]("bidsize5");
    val offerpx5 = row.getAs[Double]("offerpx5");
    val offersize5 = row.getAs[Long]("offersize5");
    val offerpx6 = row.getAs[Double]("offerpx6");
    val offersize6 = row.getAs[Long]("offersize6");
    val bidpx6 = row.getAs[Double]("bidpx6");
    val bidsize6 = row.getAs[Long]("bidsize6");
    val bidpx7 = row.getAs[Double]("bidpx7");
    val bidsize7 = row.getAs[Long]("bidsize7");
    val offerpx7 = row.getAs[Double]("offerpx7");
    val offersize7 = row.getAs[Long]("offersize7");
    val offerpx8 = row.getAs[Double]("offerpx8");
    val offersize8 = row.getAs[Long]("offersize8");
    val bidpx8 = row.getAs[Double]("bidpx8");
    val bidsize8 = row.getAs[Long]("bidsize8");
    val bidpx9 = row.getAs[Double]("bidpx9");
    val bidsize9 = row.getAs[Long]("bidsize9");
    val offerpx9 = row.getAs[Double]("offerpx9");
    val offersize9 = row.getAs[Long]("offersize9");
    val offerpx10 = row.getAs[Double]("offerpx10");
    val offersize10 = row.getAs[Long]("offersize10");
    val bidpx10 = row.getAs[Double]("bidpx10");
    val bidsize10 = row.getAs[Long]("bidsize10");

    println("-------------------77777------------------")
    /*var tpc:String=
      trphaseCode match{
        case "S0" =>"0";case "O0" =>"1";case "T0" =>"3";case "C0" =>"5";case "E0" =>"6";case "A0"=>"7";case "H0"=>"8";case "V0"=>"9";
       case "S1"=>"8";case "O1"=>"8";case "B1"=>"8";case "T1"=>"8";case "C1"=>"8";case "E1"=>"8";
       case "A1"=>"8";case "H1"=>"8";case  "V1"=>"8";
        case "S"=>"0";case "O"=>"1";case "T"=>"3";case "C"=>"5";case "E"=>"6";case "A"=>"7";case "H"=>"8";case "V"=>"9";
       case "B0"=>"4";
      };
    val hm=origtime.substring(0,4)
     if((hm>="0925" && hm<="0930")){
       tpc="2"
     }
     if((hm>="1130" && hm<="1300")){
      tpc="4"
    }*/

    val HTSCSecurityID = secid + ".SZ"
    val rowKey: String = MD5Hash.getMD5AsHex(HTSCSecurityID.getBytes()).substring(0, 6) + HTSCSecurityID + (java.lang.Long.MAX_VALUE - origt.getTime)

    //val props = rb.getString(HTSCSecurityID)
    //val str = props.trim.split(",")
    val sectype = row.getAs[String]("sectype");
    val subType = row.getAs[String]("secsubtype");
    val symbol = row.getAs[String]("symbol");
    val p = new Put(Bytes.toBytes(rowKey))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("MDDate"), Bytes.toBytes(MDDate))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("MDTime"), Bytes.toBytes(origtime))
    //  p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("MDStreamID"), Bytes.toBytes(mdstreamid))
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
    //p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("TradingPhaseCode"), Bytes.toBytes(tpc))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("PreClosePx"), Bytes.toBytes(preclosepx.toString))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("NumTrades"), Bytes.toBytes(numtrades.toString))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("TotalVolumeTrade"), Bytes.toBytes(totalVolumnTrade.toString))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("TotalValueTrade"), Bytes.toBytes(totalValueTrade.toString))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("LastPx"), Bytes.toBytes(lastpx.toString))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("OpenPx"), Bytes.toBytes(openpx.toString))
    // p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("ClosePx"), Bytes.toBytes())
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("HighPx"), Bytes.toBytes(highpx.toString))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("LowPx"), Bytes.toBytes(lowpx.toString))
    // p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("DiffPx1"), Bytes.toBytes(pxchange1))
    // p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("DiffPx2"), Bytes.toBytes(pxchange2))
    // p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("MaxPx"), Bytes.toBytes(uplimitpx))
    //  p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("MinPx"), Bytes.toBytes(downlimitpx))
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

    //---

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
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("Buy8OrderQty"), Bytes.toBytes(bidsize8))
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
