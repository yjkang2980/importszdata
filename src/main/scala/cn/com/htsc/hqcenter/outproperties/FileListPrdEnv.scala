package cn.com.htsc.hqcenter.outproperties

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
object FileListPrdEnv {

  val hdfs = org.apache.hadoop.fs.FileSystem.get(new java.net.URI("hdfs://nameservice1"), new org.apache.hadoop.conf.Configuration())


  val snapfls: Array[FileStatus] = hdfs.listStatus(new Path("hdfs://nameservice1/user/u010571/mddata/snapshotnew"))
  val snaplevelfls: Array[FileStatus] = hdfs.listStatus(new Path("hdfs://nameservice1/user/u010571/mddata/snapshotlevelnew"))
  val orderfls: Array[FileStatus] = hdfs.listStatus(new Path("hdfs://nameservice1/user/u010571/mddata/ordernew"))
  val tradefls: Array[FileStatus] = hdfs.listStatus(new Path("hdfs://nameservice1/user/u010571/mddata/tradenew"))
  val indexfls: Array[FileStatus] = hdfs.listStatus(new Path("hdfs://nameservice1/user/u010571/mddata/indexnew"))
  val stockinfofls: Array[FileStatus] = hdfs.listStatus(new Path("hdfs://nameservice1/user/u010571/mddata/stockinfonew"))
  val stockstatusfls: Array[FileStatus] = hdfs.listStatus(new Path("hdfs://nameservice1/user/u010571/mddata/stockstatusnew"))

  val snapt = "table_app_t_md_snap"
  val snaplevelt = "table_app_t_md_snap_level"
  val ordert = "table_app_t_md_order"
  val tradet = "table_app_t_md_trade"
  val  indext="table_app_t_md_index"
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("hqdataimportfl")
    val sc = new SparkContext(conf)
    val hiveContext = new HiveContext(sc)
    //val sqlContext = new SQLContext(sc)

    hiveContext.sql("use mdc")
    val startDay="20160830"
    val endDay="20160831"
    //处理分区
    dealWithPartition(hiveContext,"20160830","20160831")

    val day = getDayList(hiveContext,hdfs,snapfls)
    for(d<-day){
       if(d>="20160915" && d<="20160927"){
         val y=d.substring(0,4)
         val m=d.substring(4,6)
         val dd=d.substring(6,8)
             checktheDayIsOK(hiveContext,y,m,dd)
       }
    }
  }

  def dealWithPartition(hiveContext: HiveContext,startDay:String,endDay:String): Unit ={
    //遍历添加snapshot的数据
    addAndDropPartition(hiveContext,hdfs,snapfls,snapt,startDay,endDay)

    //遍历添加snap_level的行情
    addAndDropPartition(hiveContext,hdfs,snaplevelfls,snaplevelt,startDay,endDay)

    //遍历order行情
    addAndDropPartition(hiveContext,hdfs,orderfls,ordert,startDay,endDay)

    //遍历trade
    addAndDropPartition(hiveContext,hdfs,tradefls,tradet,startDay,endDay)

    //遍历index
    addAndDropPartition(hiveContext,hdfs,indexfls,indext,startDay,endDay)
  }

  def checktheDayIsOK(hiveContext: HiveContext,year:String,mon:String,day:String)={

    println("-------------------------------------------------开始校验"+year+"/"+mon+"/"+day+"的数据"+"-----------------------------------------------")
    val numsecId="select count(distinct(a.securityid)),count(*) from table_app_t_md_snap a where a.year='"+year+"' and month='"+mon+"' and day='"+day+"'"
    val numseclevelId="select count(distinct(a.securityid)),count(*) from table_app_t_md_snap_level a where a.year='"+year+"' and month='"+mon+"' and day='"+day+"'"
     val numRAndsecids=hiveContext.sql(numsecId)
     val secids=numRAndsecids.head(1)(0).get(0)
     val numtotal=numRAndsecids.head(1)(0).get(1)
    val numlevelt=hiveContext.sql(numseclevelId)

    println("有行情快照"+numtotal+" 行,securityIDs数量："+secids+" 十档行情数据"+numlevelt.head(1)(0).get(1)+" securityIDs数量:"+numlevelt.head(1)(0).get(0))

    checkPriceSnapIsOK(hiveContext,year,mon,day);

    checkPriceTradeNumIncrease(hiveContext,year,mon,day)

    checkTradeNumValueGtZero(hiveContext,year,mon,day)

    checkPriceVolumnAllExists(hiveContext,year,mon,day)

    checkPriceStepDown(hiveContext,year,mon,day)

    checktradNumValueSumEqualLastRec(hiveContext,year,mon,day)

    checkTradeExistsInOrder(hiveContext,year,mon,day)

    checkOrderRec(hiveContext,year,mon,day)

    checkindexRec(hiveContext,year,mon,day)

    checkTradeisOk(hiveContext,year,mon,day)
    println("-------------------------------------------------结束校验"+year+"/"+mon+"/"+day+"的数据"+"-----------------------------------------------")
  }

  def checkOrderRec(hiveContext: HiveContext,year:String,mon:String,day:String)={
     val orderQtyPriceSql="select count(*)from table_app_t_md_order t where (t.price<0 or t.price>99999999 or t.orderqty<0) and t.price!=-1.0 and " +
       "t.year='"+year+"' and t.month='"+mon+"' and t.day='"+day+"' "
     val orderQtyPriceRes=hiveContext.sql(orderQtyPriceSql)
     println("委托价格委托数量符合价格字段、成交字段规范,不符合规则的记录数："+orderQtyPriceRes.head(1)(0).get(0))

     val timeAndIndexIncreaseSql="select count(*) from (" +
       "select t.securityid,t.origtime,t.applseqnum,lag(t.origtime,1) over(partition by t.securityid order by t.applseqnum asc) " +
       "as lastRecTime,lag(t.applseqnum,1) over (partition by t.securityid order by t.applseqnum asc) as lastseqNum" +
       " from table_app_t_md_order t where t.year='"+year+"' and t.month='"+mon+"' and t.day='"+day+"') AA where AA.applseqnum<=AA.lastseqNum  " +
       "or AA.origtime<AA.lastRecTime"
    val timeAndIndexIncrease=hiveContext.sql(timeAndIndexIncreaseSql)
    println("委托时间和委托索引递增，不符合的记录数："+timeAndIndexIncrease.head(1)(0).get(0))
  }


  def checkindexRec(hiveContext: HiveContext,year:String,mon:String,day:String)={
    val numPricUnOK:DataFrame= hiveContext.sql("select count(*)from table_app_t_md_index t where t.year='"+year+"' and t.month='"+mon
      +"' and t.day='"+day+"' and " +
      "(cast(t.preclosepx as double)<0 or cast(t.preclosepx as double)>99999999 " +
      "or cast(t.lastpx as double)<0 or  cast(t.lastpx as double)>99999999 or " +
      "cast(t.openpx as double)<0 or  cast(t.openpx as double)>99999999 or " +
      "cast(t.highpx as double)<0 or  cast(t.highpx as double)>99999999 or " +
      "cast(t.lowpx as double)<0 or  cast(t.lowpx as double)>99999999 )")
    //numPricUnOK.head(5).foreach(println)
    val r=numPricUnOK.head(1)(0).get(0)
    println("指数index行情数据价格不合理记录数："+r)

    val totalVolumeTradesql="select count(*)from table_app_t_md_index " +
      "t where t.totalvaluetrade<0 or t.totalvolumetrade<0 and " +
      " t.year='"+year+"' and t.month='"+mon+"' and t.day='"+day+"' "
    val volumValueGt0=hiveContext.sql(totalVolumeTradesql)
    println("指数index行情数据totalvaluetrade,totalvolumetrade符合成交规范，不合理记录数："+volumValueGt0.head(1)(0).get(0))

    val valuetradeincreatesql="select count(*) from(" +
      "select a.securityid,  cast(a.origtime as bigint) as origtime," +
      " cast(a.totalvaluetrade as double) as totalvaluetrade," +
      " cast(a.totalvolumetrade as bigint) as totalvolumetrade," +
      "cast(lag(a.origtime,1) over (partition by a.securityid order by origtime asc)  as bigint)" +
      "as lastrecord ,cast(lag(a.totalvolumetrade,1) over (partition by a.securityid order by" +
      " origtime asc) as bigint) as lasttotalvolumetrade,cast(lag(a.totalvaluetrade,1) " +
      "over (partition by a.securityid order by origtime asc) as double) as lasttotalvaluetrade " +
      "from table_app_t_md_index a where  a.year='"+year+"'  and a.month='"+mon+"' and a.day='"+day+"' )" +
      " AA where AA.origtime <AA.lastrecord or AA.totalvaluetrade<AA.lasttotalvaluetrade " +
      "or AA.totalvolumetrade<AA.lasttotalvolumetrade"
    val valuetradeincreaseGt0=hiveContext.sql(valuetradeincreatesql)
    println("指数index行情数据totalvaluetrade,totalvolumetrade,origtime递增，不合理记录数："+valuetradeincreaseGt0.head(1)(0).get(0))
  }


  def checkTradeisOk(hiveContext: HiveContext,year:String,mon:String,day:String): Unit ={
       val tradePriceQtysql="select count(*)from table_app_t_md_trade q where q.exectype='F' and (q.price<=0 or q.tradeqty<=0) and " +
         "q.year='"+year+"' and q.month='"+mon+"' and q.day='"+day+"'"
    val valueNumTradeGt0=hiveContext.sql(tradePriceQtysql)
    println("trade行情数据成交时，价格，数量大于0，不合理记录数："+valueNumTradeGt0.head(1)(0).get(0))
    val origtimeseqnumincreasesql="select count(*) from (" +
      "select q.securityid,q.origtime,q.applseqnum," +
      "lag(q.applseqnum,1) over(partition by q.securityid order by q.applseqnum asc) as lastseqnum," +
      "lag(q.origtime,1) over(partition by q.securityid order by q.applseqnum asc) as lastorigtime" +
      " from table_app_t_md_trade q where q.year='"+year+"' and q.month='"+mon+"' and q.day='"+day+"') " +
      "AA where AA.origtime<AA.lastorigtime or AA.applseqnum<=AA.lastseqnum"
    val origtimeseqnumincrease=hiveContext.sql(origtimeseqnumincreasesql)
    println("trade行情数据，委托索引递增，数据时间递增，不合理记录数："+origtimeseqnumincrease.head(1)(0).get(0))

  }


  def checktradNumValueSumEqualLastRec(hiveContext: HiveContext,year:String,mon:String,day:String)={
    val tradepricesql="select *from " +
      "(select a.securityid,max(a.totalvolumetrade) as maxa,max(a.totalvaluetrade) as maxv " +
      "from table_app_t_md_snap a  where " +
      "  a.year='"+year+"'  and a.month='"+mon+"' and a.day='"+day+"' " +
      " group by a.securityid) AA" +
      ",(select sum(t.tradeqty) as qt,sum(t.price*t.tradeqty) as pt,t.securityid  from table_app_t_md_trade t where" +
      "  t.exectype='F' and   " +
      " t.year='"+year+"' and t.month='"+mon+"' and t.day='"+day+"' " +
      " group by t.securityid) BB " +
      " where AA.securityid=BB.securityid and AA.maxa!=BB.qt and AA.maxv!=BB.pt "
     println("trade交sql："+tradepricesql)
    val tradePS = hiveContext.sql(tradepricesql)
    println("成交记录之和应该等于每个id的最后一条记录的成交数、成交量，不符合记录数："+ tradePS.head(1)(0).get(0))
  }


  def checkPriceVolumnAllExists(hiveContext: HiveContext,year:String,mon:String,day:String) ={
    val offerbid1="select count(*)from table_app_t_md_snap_level t where" +
      " (" +
      "(cast(t.bidpx1 as double)!=0.0 and cast(t.bidsize1 as bigint)==0) or" +
      "    (cast(t.bidpx2 as double)!=0.0 and cast(t.bidsize2 as bigint)==0) or" +
      "    (cast(t.bidpx3 as double)!=0.0 and cast(t.bidsize3 as bigint)==0) or " +
      "  (cast(t.bidpx4 as double)!=0.0 and cast(t.bidsize4 as bigint)==0) or " +
      " (cast(t.bidpx5 as double)!=0.0 and cast(t.bidsize5 as bigint)==0) or " +
      "   (cast(t.bidpx6 as double)!=0.0 and cast(t.bidsize6 as bigint)==0) or " +
      "  (cast(t.bidpx7 as double)!=0.0 and cast(t.bidsize7 as bigint)==0) or " +
      "  (cast(t.bidpx8 as double)!=0.0 and cast(t.bidsize8 as bigint)==0) or " +
      "   (cast(t.bidpx9 as double)!=0.0 and cast(t.bidsize9 as bigint)==0) or " +
      " (cast(t.bidpx10 as double)!=0.0 and cast(t.bidsize10 as bigint)==0)" +
      "  or (cast(t.offerpx1 as double)!=0.0 and cast(t.offersize1 as bigint)==0) or" +
      "      (cast(t.offerpx2 as double)!=0.0 and cast(t.offersize2 as bigint)==0) or" +
      "      (cast(t.offerpx3 as double)!=0.0 and cast(t.offersize3 as bigint)==0) or" +
      "      (cast(t.offerpx4 as double)!=0.0 and cast(t.offersize4 as bigint)==0) or" +
      "     (cast(t.offerpx5 as double)!=0.0 and cast(t.offersize5 as bigint)==0) or" +
      "  (cast(t.offerpx6 as double)!=0.0 and cast(t.offersize6 as bigint)==0) or" +
      "     (cast(t.offerpx7 as double)!=0.0 and cast(t.offersize7 as bigint)==0) or" +
      "   (cast(t.offerpx8 as double)!=0.0 and cast(t.offersize8 as bigint)==0) or" +
      "      (cast(t.offerpx9 as double)!=0.0 and cast(t.offersize9 as bigint)==0) or" +
      "  (cast(t.offerpx10 as double)!=0.0 and cast(t.offersize10 as bigint)==0) " +
      "     )    and  t.year='"+year+"' and month='"+mon+"' and day='"+day+"'"
    " and (t.origtime between '"+year+mon+day+"093000000' and '"+year+mon+day+"113000000' or" +
      " t.origtime between '"+year+mon+day+"130000000' and '"+year+mon+day+"145700000' )"
    val offerbidnum1=hiveContext.sql(offerbid1).head(1)(0).get(0)
    println("十档行情里面价位存在，但是委托量为0的记录数："+offerbidnum1)
  }


  def checkTradeNumValueGtZero(hiveContext: HiveContext,year:String,mon:String,day:String) ={
    val volTradeNumsql="select count(*)from table_app_t_md_snap t " +
      "where (cast(t.numtrades as bigint) <0 or cast(t.totalvaluetrade as double)< 0 " +
      "or cast(t.totalvolumetrade as bigint)<0) and year='"+year+"' and month='"+mon+"' and day='"+day+"'";
    val volTradeNum=hiveContext.sql(volTradeNumsql)
    val vtns=volTradeNum.head(1)(0).get(0)
    println("快照数据数据NumTrades、TotalVolumeTrade、TotalValueTrade小于0的记录数："+vtns)
  }

  def checkPriceTradeNumIncrease(hiveContext: HiveContext,year:String,mon:String,day:String)={
    val volumnTradeEtc="select count(*) from(" +
      "select a.securityid , cast(a.origtime as bigint) as origtime," +
      "  cast(a.numtrades as bigint) as numtrades, cast(a.totalvolumetrade as bigint) as totalvolumetrade," +
      " cast(a.totalvaluetrade as bigint) as totalvaluetrade," +
      "cast(lag(a.origtime,1) over (partition by a.securityid order by a.origtime asc) as bigint)as lastrecord ," +
      "cast(lag(a.numtrades,1) over (partition by a.securityid order by a.origtime asc) as bigint)as lastnumtrade ," +
      "cast(lag(a.totalvolumetrade,1) over (partition by a.securityid order by a.origtime asc) as bigint) as lasttotalvolumetrade," +
      "cast(lag(a.totalvaluetrade,1) over (partition by a.securityid order by a.origtime asc)as bigint) as lasttotalvaluetrade " +
      "from table_app_t_md_snap a where a.year='"+year+"' and month='"+mon+"' and day='"+day+"'   " +
      ") as AA    " +
      " where AA.origtime<=AA.lastrecord and AA.numtrades<AA.lastnumtrade and AA.totalvolumetrade<AA.lasttotalvolumetrade " +
      "and AA.totalvaluetrade<AA.lasttotalvaluetrade"
    val volumTradeOK:DataFrame= hiveContext.sql(volumnTradeEtc)
    val vt=volumTradeOK.head(1)(0).get(0)
    println("快照数据数据时间、成交笔数、成交总量、成交总金额没有递增的记录数："+vt)
  }

  def checkPriceSnapIsOK(hiveContext: HiveContext,year:String,mon:String,day:String)={
    val numPricUnOK:DataFrame= hiveContext.sql("select count(*)from table_app_t_md_snap t where t.year='"+year+"' and t.month='"+mon
      +"' and t.day='"+day+"' and " +
      "(cast(t.preclosepx as double)<0 or cast(t.preclosepx as double)>99999999 " +
      "or cast(t.lastpx as double)<0 or  cast(t.lastpx as double)>99999999 or " +
      "cast(t.openpx as double)<0 or  cast(t.openpx as double)>99999999 or " +
      "cast(t.highpx as double)<0 or  cast(t.highpx as double)>99999999 or " +
      "cast(t.lowpx as double)<0 or  cast(t.lowpx as double)>99999999 or " +
      "cast(t.highpx as double) <  cast(t.lastpx as double) or cast(t.lowpx as double) >cast(t.lastpx as double))")
    val r=numPricUnOK.head(1)(0).get(0)
    println("快照数据价格不合理记录数："+r)
  }


  def checkPriceStepDown(hiveContext: HiveContext,year:String,mon:String,day:String)={

    val priceordersql="select t.bidpx10,t.bidpx9,t.bidpx8,t.bidpx7,t.bidpx6,t.bidpx5,t.bidpx4,t.bidpx3,t.bidpx2,t.bidpx1,t.offerpx1,t.offerpx2,t.offerpx3,t.offerpx4,t.offerpx5,t.offerpx6,t.offerpx7,t.offerpx8,t.offerpx9,t.offerpx10,t.securityid" +
      " from table_app_t_md_snap_level t where  " +
       " (t.origtime between '"+year+mon+day+"093000000' and '"+year+mon+day+"113000000' or" +
      " t.origtime between '"+year+mon+day+"130000000' and '"+year+mon+day+"145700000' )" +
      "  and t.year='"+year+"' and t.month='"+mon+"' and t.day='"+day+"'"
    //println("价格递减的sql:"+priceordersql)
    val priceresult=hiveContext.sql(priceordersql);
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



  def checkTradeExistsInOrder(hiveContext: HiveContext,year:String,mon:String,day:String)={
    val existsOrder="select count(distinct(AA.tid)) from(" +
      " select t.bidapplseqnum as tid from table_app_t_md_trade t where t.bidapplseqnum!='0'" +
      "  and t.year='"+year+"' and t.month='"+mon+"' and t.day='"+day+"' " +
      " union all " +
      "  select q.offerapplseqnum as tid from table_app_t_md_trade q where q.offerapplseqnum!='0'" +
      "  and q.year='"+year+"' and q.month='"+mon+"' and q.day='"+day+"'" +
      ") AA left join " +
      "  table_app_t_md_order p on AA.tid=p.applseqnum where " +
      " p.year='"+year+"' and p.month='"+mon+"' and p.day='"+day+"' and p.applseqnum is null "
    println("existsOrder:"+existsOrder)
    val tradeOrderexists=hiveContext.sql(existsOrder)
    println("成交序列号必须在委托序列号里面,不符的成交序列号个数："+tradeOrderexists.head(1)(0).get(0))
  }


  /**
    * 遍历添加hive表外部分区的
    * @param hiveContext
    * @param hdfs
    * @param p1
    * @param tablename
    */
  def addAndDropPartition(hiveContext: HiveContext,hdfs: FileSystem,p1: Array[FileStatus],tablename:String,startDay:String,endDay:String) = {

    for (fs <- p1) {
      val yearPathStr = fs.getPath.toString;
      val yearstr = yearPathStr.substring(yearPathStr.length - 4, yearPathStr.length)
      //val monthlist=hdfs.listStatus(sy.getPath)

      val monlist = hdfs.listStatus(fs.getPath)
      for (sy <- monlist) {
        val monPathStr = sy.getPath.toString;
        val monstr = monPathStr.substring(monPathStr.length - 2, monPathStr.length)
        val daylist = hdfs.listStatus(sy.getPath)
        for (day <- daylist) {
          val dayPathStr = day.getPath.toString;
          val daystr = dayPathStr.substring(dayPathStr.length - 2, dayPathStr.length)
          println("分区:" + "Y:" + yearstr + " M:" + monstr + " D:" + daystr)
          val location = yearstr  + monstr  + daystr;
          val location1 = yearstr +"/" + monstr+"/"+ daystr;
          if(location>=startDay && location<=endDay) {
            val dropp = "ALTER TABLE " + tablename + " DROP IF EXISTS PARTITION (year='" + yearstr + "', month='" + monstr + "', day='" + daystr + "')"
            println(" 如果已有删除分区： " + location)
            hiveContext.sql(dropp)
            val strsql = "alter table " + tablename + " add partition (year='" + yearstr + "', month='" + monstr + "', day='" + daystr + "') location '" + location1 + "'"
            println("分区语句：" + strsql)
            hiveContext.sql(strsql)
            println("已完成分区添加!" + "分区:" + "Y:" + yearstr + " M:" + monstr + " D:" + daystr)
          }
        }
      }

    }
  }

  //def subDir(dir: Path): Iterator[FileStatus] = {
  //  val hdfs = org.apache.hadoop.fs.FileSystem.get(new java.net.URI("hdfs://nameservice1:9000"), new org.apache.hadoop.conf.Configuration())

  // val dirs = List(dir).filter(_.isDirectory())
  // val files = dir.listFiles().filter(_.isFile())
  //files.toIterator ++ dirs.toIterator.flatMap(subDir _)
  //val fl=hdfs.listStatus(dir)


  //}

  /**
    * 校验每一天的数据是否符合逻辑
    * @param hiveContext
    * @param hdfs
    * @param p1  example: val snapfls: Array[FileStatus] = hdfs.listStatus(new Path("hdfs://nameservice1/user/u010571/snapshotnew"))
    */
  def getDayList(hiveContext: HiveContext,hdfs: FileSystem,p1: Array[FileStatus]):List[String] = {

    var days: List[String] = List()

    for (fs <- p1) {
     // val yearPathStr = fs.getPath.toString;
      //val yearstr = yearPathStr.substring(yearPathStr.length - 4, yearPathStr.length)
      //val monthlist=hdfs.listStatus(sy.getPath)
      val yearPathStr = fs.getPath.toString;
      val yearstr = yearPathStr.substring(yearPathStr.length - 8, yearPathStr.length)
      //val monthlist=hdfs.listStatus(sy.getPath)

      days = yearstr +: days
    }
    days
  }


  def dropPartition(hiveContext: HiveContext,startDate:String,endDate:String){
    //遍历添加snapshot的数据
    dropPartitionOld(hiveContext,hdfs,snapfls,snapt,startDate,endDate)

    //遍历添加snap_level的行情
    dropPartitionOld(hiveContext,hdfs,snaplevelfls,snaplevelt,startDate,endDate)

    //遍历order行情
    dropPartitionOld(hiveContext,hdfs,orderfls,ordert,startDate,endDate)

    //遍历trade
    dropPartitionOld(hiveContext,hdfs,tradefls,tradet,startDate,endDate)

    //遍历index
    dropPartitionOld(hiveContext,hdfs,indexfls,indext,startDate,endDate)
  }

  def dropPartitionOld(hiveContext: HiveContext,hdfs: FileSystem,p1: Array[FileStatus],tablename:String,startDate:String,endDate:String) = {

    for (fs <- p1) {
      val yearPathStr = fs.getPath.toString;
      val yearstr = yearPathStr.substring(yearPathStr.length - 4, yearPathStr.length)
      //val monthlist=hdfs.listStatus(sy.getPath)

      val monlist = hdfs.listStatus(fs.getPath)
      for (sy <- monlist) {
        val monPathStr = sy.getPath.toString;
        val monstr = monPathStr.substring(monPathStr.length - 2, monPathStr.length)
        val daylist = hdfs.listStatus(sy.getPath)
        for (day <- daylist) {
          val dayPathStr = day.getPath.toString;
          val daystr = dayPathStr.substring(dayPathStr.length - 2, dayPathStr.length)
          println("分区:" + "Y:" + yearstr + " M:" + monstr + " D:" + daystr)
          val location = yearstr + "/" + monstr + "/" + daystr;
          val parstr= yearstr + monstr + daystr
          if(parstr>=startDate && parstr<=endDate) {
            val dropp = "ALTER TABLE " + tablename + " DROP IF EXISTS PARTITION (year='" + yearstr + "', month='" + monstr + "', day='" + daystr + "')"
            println(" 如果已有删除分区： " + location)
            hiveContext.sql(dropp)
            hdfs.delete(day.getPath,true);
            println("已完成分区删除!" + "分区:" + "Y:" + yearstr + " M:" + monstr + " D:" + daystr)
          }
        }
      }

    }
  }


}
