package cn.com.htsc.hqcenter.outproperties.shlv

import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.control._

/**
  *
  * @author
  * @version $Id:
  *
  */
object FileListPrdOldEnvshlv {

  val hdfs = org.apache.hadoop.fs.FileSystem.get(new java.net.URI("hdfs://nameservice1"), new org.apache.hadoop.conf.Configuration())

 // val snapfls: Array[FileStatus] = hdfs.listStatus(new Path("hdfs://nameservice1/user/u010571/mddata/snapshotold"))
  val snaplevelfls: Array[FileStatus] = hdfs.listStatus(new Path("hdfs://nameservice1/user/u010571/shlv2/snapshot"))
 // val orderfls: Array[FileStatus] = hdfs.listStatus(new Path("hdfs://nameservice1/user/u010571/mddata/orderold"))
  val tradefls: Array[FileStatus] = hdfs.listStatus(new Path("hdfs://nameservice1/user/u010571/shlv2/tradeticker"))
  //val indexfls: Array[FileStatus] = hdfs.listStatus(new Path("hdfs://nameservice1/user/u010571/mddata/indexold"))
  //val stockinfofls: Array[FileStatus] = hdfs.listStatus(new Path("hdfs://nameservice1/user/u010571/mddata/stockinfoold"))
  //val stockstatusfls: Array[FileStatus] = hdfs.listStatus(new Path("hdfs://nameservice1/user/u010571/mddata/stockstatusold"))

  //val snapt = "table_app_t_md_snap_old"
  val snaplevelt = "table_app_t_md_snap_level_shlv"
  val tradet = "table_app_t_md_trade_shlv"
  val snapleveltOrig = "table_app_t_md_snap_level_shlv_orig"
  val tradetOrig = "table_app_t_md_trade_shlv_orig"
  //val  indext="table_app_t_md_index_old"



  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("hqshlv2dataimport")
    val sc = new SparkContext(conf)
    val hiveContext = new HiveContext(sc)
    hiveContext.sql("use mdc")
    val sqlContext = new SQLContext(sc)


    val loop = new Breaks;



    dropAndAddPartition(hiveContext,"20151211","20150131")

    val days=getDayList(hiveContext,hdfs,snaplevelfls)
    val startd="20150301";val endd="20150331"
    for(day<-days){
      if(day >=startd && day <=endd){
        var year=day.substring(0,4)
        var mon=day.substring(4,6)
        var d= day.substring(6,8)
        checktheDayIsOKOld(hiveContext,year+mon+d)
      }
    }

    sc.stop()
  }


   def dropPartition(hiveContext: HiveContext,startDate:String,endDate:String){
     //遍历添加snapshot的数据
     //dropPartitionOld(hiveContext,hdfs,snapfls,snapt,startDate,endDate)

     //遍历添加snap_level的行情
     dropPartitionOld(hiveContext,hdfs,snaplevelfls,snaplevelt,startDate,endDate)

     //遍历order行情
    // dropPartitionOld(hiveContext,hdfs,orderfls,ordert,startDate,endDate)

     //遍历trade
     dropPartitionOld(hiveContext,hdfs,tradefls,tradet,startDate,endDate)

     //遍历index
    // dropPartitionOld(hiveContext,hdfs,indexfls,indext,startDate,endDate)
  }

  def dropAndAddPartition(hiveContext: HiveContext,startDate:String,endDate:String)={
    //遍历添加snapshot的数据
    //addAndDropPartitionOld(hiveContext,hdfs,snapfls,snapt,startDate,endDate)

    //遍历添加snap_level的行情
    addAndDropPartitionOld(hiveContext,hdfs,snaplevelfls,snapleveltOrig,startDate,endDate)

    //遍历order行情
    //addAndDropPartitionOld(hiveContext,hdfs,orderfls,ordert,startDate,endDate)

    //遍历trade
    addAndDropPartitionOld(hiveContext,hdfs,tradefls,tradetOrig,startDate,endDate)

    //遍历index
    //addAndDropPartitionOld(hiveContext,hdfs,indexfls,indext,startDate,endDate)
  }

  def checktheDayIsOKOld(hiveContext: HiveContext,ymd:String)={

    println("-------------------------------------------------开始校验"+ymd+"的数据"+"-----------------------------------------------")
    val numsecId="select count(distinct(a.securityid)),count(*) from table_app_t_md_snap_level_shlv a where a.ymd='"+ymd+"'"
    //val numseclevelId="select count(distinct(a.securityid)),count(*) from table_app_t_md_snap_level_old a where a.year='"+year+"' and month='"+mon+"' and day='"+day+"'"
     val numRAndsecids=hiveContext.sql(numsecId)
     val secids=numRAndsecids.head(1)(0).get(0)
     val numtotal=numRAndsecids.head(1)(0).get(1)
    //val numlevelt=hiveContext.sql(numseclevelId)

    println("有行情快照"+numtotal+" 行,securityIDs数量："+secids)

    //9：25 14：59有一部分最高最低开盘都是0.0的数据
    checkPriceSnapIsOKOld(hiveContext,ymd);

    //快照时间仅精确到秒，有一些securityid同一秒两条快照,有一条成交额反而少了
    checkPriceTradeNumIncreaseOld(hiveContext,ymd)

    checkPriceVolumnAllExistsOld(hiveContext,ymd)

    checkPriceStepDownOld(hiveContext,ymd)

    checktradNumValueSumEqualLastRecOld(hiveContext,ymd)

    //从这个函数开始总是溢出......
    //checkTradeExistsInOrderOld(hiveContext,year,mon,day)

    //checkOrderRecOld(hiveContext,year,mon,day)

    //checkindexRecOld(hiveContext,year,mon,day)

    checkTradeisOkOld(hiveContext,ymd)
    println("-------------------------------------------------结束校验"+ymd+"的数据"+"-----------------------------------------------")
  }

  //OK!
  def checkOrderRecOld(hiveContext: HiveContext,year:String,mon:String,day:String)={
     val orderQtyPriceSql="select count(*)from table_app_t_md_order_old t where (t.price<0 or t.price>99999999 or t.orderqty<0) and t.price!=-1.0 and " +
       "t.year='"+year+"' and t.month='"+mon+"' and t.day='"+day+"' "
     val orderQtyPriceRes=hiveContext.sql(orderQtyPriceSql)
     println("委托价格委托数量符合价格字段、成交字段规范,不符合规则的记录数："+orderQtyPriceRes.head(1)(0).get(0))

     val timeAndIndexIncreaseSql="select count(*)from(" +
       "select AA.securityid,AA.recno,AA.lastrecno ,AA.orderentrytime,AA.lastrectime,(AA.orderentrytime-AA.lastrectime) as difft " +
       "from(select t.securityid,t.orderentrytime,t.recno," +
       "lag(t.orderentrytime,1) over(partition by t.securityid order by t.recno asc) as lastrectime," +
       "lag(t.recno,1) over(partition by t.securityid order by t.recno asc) as lastrecno " +
       "from table_app_t_md_order_old t where t.year='"+year+"' and t.month='"+mon+"' and t.day='"+day+"') AA " +
       " ) BB where BB.recno<BB.lastrecno or BB.difft<-1000"
    val timeAndIndexIncrease=hiveContext.sql(timeAndIndexIncreaseSql)
    println("委托时间和委托索引递增，不符合的记录数："+timeAndIndexIncrease.head(1)(0).get(0))
  }

//OK!
  def checkindexRecOld(hiveContext: HiveContext,year:String,mon:String,day:String)={
    val numPricUnOK:DataFrame= hiveContext.sql("select count(*) from table_app_t_md_index_old t where" +
      " t.year='"+year+"' and t.month='"+mon+"' and t.day='"+day+"' and" +
      " (cast(t.closeindex as double)<0 or cast(t.closeindex as double)>99999999 " +
      "or cast(t.lastindex as double)<0 or  cast(t.lastindex as double)>99999999 or" +
      "   cast(t.openindex as double)<0 or  cast(t.openindex as double)>99999999 or " +
      "cast(t.highindex as double)<0 or  cast(t.highindex as double)>99999999 or" +
      "  cast(t.lowindex as double)<0 or  cast(t.lowindex as double)>99999999)")
    //numPricUnOK.head(5).foreach(println)
    val r=numPricUnOK.head(1)(0).get(0)
    println("指数index行情数据价格不合理记录数："+r)

    val totalVolumeTradesql="select count(*)from table_app_t_md_index_old " +
      "t where (t.numtrades<0 or t.totalvolumetraded<0) and " +
      " t.year='"+year+"' and t.month='"+mon+"' and t.day='"+day+"' "
    val volumValueGt0=hiveContext.sql(totalVolumeTradesql)
    println("指数index行情数据totalvaluetrade,totalvolumetrade符合成交规范，不合理记录数："+volumValueGt0.head(1)(0).get(0))

    //指数成交量有异常的，需要去除后入库
    val valuetradeincreatesql="select count(*)from(select t.securityid,t.datatimestamp,t.totalvolumetraded," +
      "lag(t.datatimestamp,1) over (partition by t.securityid order by t.datatimestamp asc,t.totalvolumetraded asc) as lasttime," +
      "lag(t.totalvolumetraded,1) over (partition by t.securityid order by t.datatimestamp asc,t.totalvolumetraded asc) as lastvol " +
      "from table_app_t_md_index_old t where t.year='"+year+"' and t.month='"+mon+"' and t.day='"+day+"')" +
      "AA where AA.datatimestamp<AA.lasttime or AA.totalvolumetraded < AA.lastvol "
    val valuetradeincreaseGt0=hiveContext.sql(valuetradeincreatesql)
    println("指数index行情数据totalvaluetrade,totalvolumetrade,origtime递增，不合理记录数："+valuetradeincreaseGt0.head(1)(0).get(0))
  }

  def checkTradeisOkOld(hiveContext: HiveContext,ymd:String): Unit ={
       val tradePriceQtysql="select  count(*)from table_app_t_md_trade_shlv t where " +
         " t.ymd='"+ymd+"' " +
         " and (t.price<=0.0 or t.tradeqty<=0)"
    val valueNumTradeGt0=hiveContext.sql(tradePriceQtysql)
    println("trade行情数据成交时，价格，数量应该大于0，不合理记录数："+valueNumTradeGt0.head(1)(0).get(0))
    if(valueNumTradeGt0.head(1)(0).getAs[Long](0)>0){
      val tradePriceQtysql2="select distinct(t.securityid)from table_app_t_md_trade_shlv t where " +
        " t.ymd='"+ymd+"'  " +
        " and (t.price<=0.0 or t.tradeqty<=0)"
      val secids=hiveContext.sql(tradePriceQtysql2)
      println("trade行情数据成交时，价格，数量应该大于0，不合理记录数大于0时的证券id："+secids.show())
    }

   /* val origtimeseqnumincreasesql="select count(*)from " +
      "(select t.securityid,t.tradetime,lag(t.tradetime,1) over(partition by t.securityid order by t.recno asc) as lasttime," +
      "t.recno, lag(t.recno,1)over (partition by t.securityid order by t.recno asc) as lastrecno " +
      "from table_app_t_md_trade_old t where t.year='"+year+"' and t.month='"+mon+"' and t.day='"+day+"' " +
      ") AA where AA.tradetime<AA.lasttime or AA.recno <=AA.lastrecno"
    val origtimeseqnumincrease=hiveContext.sql(origtimeseqnumincreasesql)
    println("trade行情数据，委托索引递增，数据时间递增，不合理记录数："+origtimeseqnumincrease.head(1)(0).get(0))

    if(origtimeseqnumincrease.head(1)(0).getLong(0) >0){
      val origtimeseqnumincreasesql1="select distinct(AA.securityid)from " +
        "(select t.securityid,t.tradetime,lag(t.tradetime,1) over(partition by t.securityid order by t.recno asc) as lasttime," +
        "t.recno, lag(t.recno,1)over (partition by t.securityid order by t.recno asc) as lastrecno " +
        "from table_app_t_md_trade_old t where t.year='"+year+"' and t.month='"+mon+"' and t.day='"+day+"' " +
        ") AA where AA.tradetime<AA.lasttime or AA.recno <=AA.lastrecno"
      val origtimeseqnumincrease1=hiveContext.sql(origtimeseqnumincreasesql1)
      println("trade行情数据，委托索引递增，数据时间递增，不合理记录大于0,对应的证券id为：")
      //origtimeseqnumincrease1.foreach(println)
      origtimeseqnumincrease1.show()
    }*/
  }

//ok!
  def checktradNumValueSumEqualLastRecOld(hiveContext: HiveContext,ymd:String)={
    val tradepricesql="select count(*)from(select t.securityid, sum(t.tradeqty) sum1,sum(t.price*t.tradeqty) sum2 " +
      " from table_app_t_md_trade_shlv t where " +
      "t.ymd='"+ymd+"' " +
      "group by t.securityid)AA, (select t.securityid, max(t.totalvolumetrade) as max1,max(t.totalvaluetrade) max2 " +
      "from table_app_t_md_snap_level_shlv t where t.ymd='"+ymd+"' group by t.securityid) " +
      "BB where AA.securityid=BB.securityid and AA.sum1!=max1 and AA.sum2!=max2"
   // println("trade交sql："+tradepricesql)
    val tradePS = hiveContext.sql(tradepricesql)
    println("成交记录之和应该等于每个id的最后一条记录的成交数、成交量，不符合记录数："+ tradePS.head(1)(0).get(0))
  }

  def checkPriceVolumnAllExistsOld(hiveContext: HiveContext,ymd:String) ={
    val offerbid1="select count(*)from table_app_t_md_snap_level_shlv t where" +
      " t.ymd='"+ymd+"'  and " +
      "((t.bidpx1!=0.0 and t.Bid1OrderQty==0) or (t.bidpx2!=0.0 and t.Bid2OrderQty==0) or" +
      " (t.bidpx3!=0.0 and t.Bid3OrderQty==0) or (t.bidpx4!=0.0 and t.Bid4OrderQty==0) or  " +
      "(t.bidpx5!=0.0 and t.Bid5OrderQty==0) or (t.bidpx6!=0.0 and t.Bid6OrderQty==0) or " +
      " (t.bidpx7!=0.0 and t.Bid7OrderQty==0) or (t.bidpx8!=0.0 and t.Bid8OrderQty==0) or" +
      " (t.bidpx9!=0.0 and t.Bid9OrderQty==0) or (t.bidpx10!=0.0 and t.Bid10OrderQty==0) or " +
      " (t.Offer1Price!=0.0 and t.Offer1OrderQty==0) or (t.Offer2Price!=0.0 and t.Offer2OrderQty==0) or" +
      "  (t.Offer3Price!=0.0 and t.Offer3OrderQty==0) or (t.Offer4Price!=0.0 and t.Offer4OrderQty==0) or" +
      " (t.Offer5Price!=0.0 and t.Offer5OrderQty==0) or (t.Offer6Price!=0.0 and t.Offer6OrderQty==0) or" +
      "  (t.Offer7Price!=0.0 and t.Offer7OrderQty==0) or (t.Offer8Price!=0.0 and t.Offer8OrderQty==0) or " +
      " (t.Offer9Price!=0.0 and t.Offer9OrderQty==0) or (t.Offer10Price!=0.0 and t.Offer10OrderQty==0))"

    println(offerbid1)
    val offerbidnum1=hiveContext.sql(offerbid1).head(1)(0).getAs[Long](0)
    println("十档行情里面价位存在，但是委托量为0的记录数："+offerbidnum1)

    if(offerbidnum1 >0){
      val offerbid2="select distinct(t.securityid) from table_app_t_md_snap_level_shlv t where" +
        " t.ymd='"+ymd+"' and " +
        "((t.bidpx1!=0.0 and t.Bid1OrderQty==0) or (t.bidpx2!=0.0 and t.Bid2OrderQty==0) or" +
        " (t.bidpx3!=0.0 and t.Bid3OrderQty==0) or (t.bidpx4!=0.0 and t.Bid4OrderQty==0) or  " +
        "(t.bidpx5!=0.0 and t.Bid5OrderQty==0) or (t.bidpx6!=0.0 and t.Bid6OrderQty==0) or " +
        " (t.bidpx7!=0.0 and t.Bid7OrderQty==0) or (t.bidpx8!=0.0 and t.Bid8OrderQty==0) or" +
        " (t.bidpx9!=0.0 and t.Bid9OrderQty==0) or (t.bidpx10!=0.0 and t.Bid10OrderQty==0) or " +
        " (t.Offer1Price!=0.0 and t.Offer1OrderQty==0) or (t.Offer2Price!=0.0 and t.Offer2OrderQty==0) or" +
        "  (t.Offer3Price!=0.0 and t.Offer3OrderQty==0) or (t.Offer4Price!=0.0 and t.Offer4OrderQty==0) or" +
        " (t.Offer5Price!=0.0 and t.Offer5OrderQty==0) or (t.Offer6Price!=0.0 and t.Offer6OrderQty==0) or" +
        "  (t.Offer7Price!=0.0 and t.Offer7OrderQty==0) or (t.Offer8Price!=0.0 and t.Offer8OrderQty==0) or " +
        " (t.Offer9Price!=0.0 and t.Offer9OrderQty==0) or (t.Offer10Price!=0.0 and t.Offer10OrderQty==0))"
      val offerbidnum2=hiveContext.sql(offerbid2)
      println("十档行情里面价位存在，但是委托量为0的记录数大于0时，证券id："+offerbidnum2.show())
    }

  }
//OK
  def checkPriceTradeNumIncreaseOld(hiveContext: HiveContext,ymd:String)={
    val ymd="20140102"
    val volumnTradeEtc="select *from(select t.securityid,t.numtrades," +
      "lag(t.numtrades,1) over(partition by t.securityid order by t.datetime asc,t.numtrades asc) as lastnumt," +
      " t.totalvaluetrade, " +
      "lag(t.totalvaluetrade,1) over (partition by t.securityid order by t.datetime asc,t.numtrades asc) as lastval," +
      "t.totalvolumetrade,lag(t.totalvolumetrade,1) over(partition by t.securityid order by t.datetime asc,t.numtrades asc) as lastvol," +
      "t.datetime,lag(t.datetime,1) over(partition by t.securityid order by t.datetime asc) as lasttime " +
      "from table_app_t_md_snap_level_shlv t where t.ymd='"+ymd+"'  ) AA where (AA.numtrades<AA.lastnumt or " +
      "AA.totalvaluetrade<AA.lastval or AA.totalvolumetrade<AA.lastvol)"
    println("sql:"+volumnTradeEtc)

    val volumTradeOK:DataFrame= hiveContext.sql(volumnTradeEtc)
    println(volumTradeOK.head(4))
    val vt=volumTradeOK.count()
    println("快照数据数据时间、成交笔数、成交总量、成交总金额没有递增的记录数："+vt)
  }

  //数据有lowpx是999999.99 同时higpx又是0的不合理
  def checkPriceSnapIsOKOld(hiveContext: HiveContext,ymd:String)={
    val sql:String="select *from table_app_t_md_snap_level_shlv t " +
      "where t.ymd='"+ymd+"' and " +
      "(cast(t.preclosepx as double)<0 or cast(t.preclosepx as double)>99999999  " +
      "or cast(t.lastpx as double)<0 or  cast(t.lastpx as double)>99999999 or " +
      "cast(t.openpx as double)<0 or  cast(t.openpx as double)>99999999 or cast(t.highpx as double)<0 or  cast(t.highpx as double)>99999999 or " +
      " cast(t.lowpx as double)<0 or  cast(t.lowpx as double)>99999999 or " +
      " cast(t.highpx as double) <  cast(t.lastpx as double) or cast(t.lowpx as double) >cast(t.lastpx as double) or " +
      " t.numtrades<0 or t.totalvolumetrade<0 or t.totalvaluetrade<0)";
    println("sql:"+sql)
    val numPricUnOK:DataFrame= hiveContext.sql(sql)

   // numPricUnOK.head(6).foreach(println)
    val r=numPricUnOK.count()
    println("快照数据价格不合理,numtrades,totalvolumetrade,totalvaluetrade不合理记录数："+r)

    if(r>0){
      val numPricSecs:DataFrame= hiveContext.sql("select distinct(t.securityid) from table_app_t_md_snap_level_shlv t " +
        "where t.ymd='"+ymd+"' and " +
        "(cast(t.preclosepx as double)<0 or cast(t.preclosepx as double)>99999999  " +
        "or cast(t.lastpx as double)<0 or  cast(t.lastpx as double)>99999999 or " +
        "cast(t.openpx as double)<0 or  cast(t.openpx as double)>99999999 or cast(t.highpx as double)<0 or  cast(t.highpx as double)>99999999 or " +
        " cast(t.lowpx as double)<0 or  cast(t.lowpx as double)>99999999 or " +
        " cast(t.highpx as double) <  cast(t.lastpx as double) or cast(t.lowpx as double) >cast(t.lastpx as double) or " +
        " t.numtrades<0 or t.totalvolumetrade<0 or t.totalvaluetrade<0)")
      println("快照数据价格不合理,numtrades,totalvolumetrade,totalvaluetrade不合理记录数大于0时，secids："+numPricSecs.show())
    }
  }

//待处理
  def checkPriceStepDownOld(hiveContext: HiveContext,ymd:String)= {

    val priceordersql = "select t.bidpx10,t.bidpx9,t.bidpx8,t.bidpx7,t.bidpx6,t.bidpx5,t.bidpx4,t.bidpx3,t.bidpx2," +
      "t.bidpx1,t.Offer1Price,t.Offer2Price,t.Offer3Price,t.Offer4Price,t.Offer5Price,t.Offer6Price,t.Offer7Price,t.Offer8Price,t.Offer9Price, " +
      "t.Offer10Price,t.securityid from table_app_t_md_snap_level_shlv t where " +
      " t.ymd='" + ymd + "' and t.securityid!='HEAD:v1'"

    println("价格递减的sql:"+priceordersql)
    val priceresult = hiveContext.sql(priceordersql);
   // println(priceresult.count())
  //}
    val resultRdd=priceresult.rdd.filter(row=>{
      var isbad=false
      for (i: Int <- 0 until 9) {
        val be = row.get(i).asInstanceOf[Double]
        val af = row.get(i + 1).asInstanceOf[Double]
        if (be == 0.0) {
          if(i>0) {
            val bebe=row.get(i-1).asInstanceOf[Double]
            if (bebe > 0.0) {
              isbad = true
            }
          }
        } else {
          if( af<=be){
            //  println("af:"+af+" be:"+be)
            isbad=true
          }
        }
      }
      if(row.get(9).asInstanceOf[Double]!=0.0 && row.get(10).asInstanceOf[Double]!=0.0){
        if(row.get(9).asInstanceOf[Double]>row.get(10).asInstanceOf[Double]){
          isbad=true
        }
      }
      for (i: Int <- 10 until 19) {
        val be = row.get(i).asInstanceOf[Double]
        val af = row.get(i + 1).asInstanceOf[Double]
        if (be == 0.0) {
          if (af > 0.0) {
            isbad=true
          }
        } else {
          if(af!=0 && af<=be){
            //  println("af:"+af+" be:"+be)
            isbad=true
          }
        }
      }
      isbad
    })
    //println(resultRdd.first())
    val ppunreal = resultRdd.count
    println("十档行情里面价格依次递减，不符合规则的记录数是："+ppunreal)
  }

//成交序列号必须在委托序列号里面
  def checkTradeExistsInOrderOld(hiveContext: HiveContext,year:String,mon:String,day:String)={
    val existsOrder="select count(*)from(select distinct(t.buyorderrecno) as tid from table_app_t_md_trade_old  t " +
      " where  t.year='"+year+"' and t.month='"+mon+"' and t.day='"+day+"' and trim(t.buyorderrecno)!='0' " +
      "union all select distinct(q.sellorderrecno) as tid  from table_app_t_md_trade_old  q " +
      "where  q.year='"+year+"' and q.month='"+mon+"' and q.day='"+day+"' and trim(q.sellorderrecno)!='0' " +
      ") AA left join table_app_t_md_order_old p on p.recno=AA.tid where " +
      "p.year='"+year+"' and p.month='"+mon+"' and p.day='"+day+"' and (p.recno is null or p.recno='')"
    println(existsOrder)

    val tradeOrderexists=hiveContext.sql(existsOrder)
    println("成交序列号必须在委托序列号里面,不符的成交序列号个数："+tradeOrderexists.head(1)(0).get(0))
  }

  def dropPartitionOld(hiveContext: HiveContext,hdfs: FileSystem,p1: Array[FileStatus],tablename:String,startDate:String,endDate:String) = {

    for (fs <- p1) {
      val yearPathStr = fs.getPath.toString;
      val ymd = yearPathStr.substring(yearPathStr.length - 8, yearPathStr.length)
      //val monthlist=hdfs.listStatus(sy.getPath)

      if(ymd>=startDate && ymd<=endDate) {
        val dropp = "ALTER TABLE " + tablename + " DROP IF EXISTS PARTITION (ymd='"+ymd+"')"
        println(" 如果已有删除分区： " + ymd)
        hiveContext.sql(dropp)

        hdfs.delete(fs.getPath,true);

        println("已完成分区删除!" + "分区:" +ymd)
      }

    }
  }

  /**
    * 遍历添加hive表外部分区的
    * @param hiveContext
    * @param hdfs
    * @param p1
    * @param tablename
    */
  def addAndDropPartitionOld(hiveContext: HiveContext,hdfs: FileSystem,p1: Array[FileStatus],tablename:String,startDate:String,endDate:String) = {

    for (fs <- p1) {
      val yearStr = fs.getPath.toString;
      val yearPathStr = yearStr.substring(yearStr.length - 8, yearStr.length)
          if(yearPathStr>=startDate && yearPathStr<=endDate) {
            val dropp = "ALTER TABLE " + tablename + " DROP IF EXISTS PARTITION (ymd='" + yearPathStr + "')"
            println(" 如果已有删除分区： " + yearPathStr)
            hiveContext.sql(dropp)
            val strsql = "alter table " + tablename + " add partition (ymd='" + yearPathStr + "') location '"+yearPathStr+"'"
            println("分区语句：" + strsql)
            hiveContext.sql(strsql)
            println("已完成分区添加!" + "分区:" +yearPathStr)
      }

    }
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
      val yearstr = yearPathStr.substring(yearPathStr.length - 8, yearPathStr.length)
      //val monthlist=hdfs.listStatus(sy.getPath)

      days = yearstr +: days
    }
    days
  }




}
