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
  *
  */
object InsetIndexIntoHBasePrdEnvOldFormat {

  def main(args: Array[String]): Unit = {
 val conf = new SparkConf().setAppName("hqdataimport")
    val sc = new SparkContext(conf)
    println("1-------------------------------------")
    val hiveContext = new HiveContext(sc)
    hiveContext.sql("use mdc")
    val sqlContext = new SQLContext(sc)
    val rb = ResourceBundle.getBundle("prdInfo".trim)
    val rb1=ResourceBundle.getBundle("option")
    val zkQurom=rb1.getString("zkQurom")
    val keys=rb.getKeys
    val prdinfoRdd=sc.textFile("hdfs://nameservice1/user/u010571/data/prdInfo.txt")

    val newprd=prdinfoRdd.map(row=>{
    Row(row.split(",")(0),row.split(",")(1),row.split(",")(2),row.split(",")(3))
    })

    val structType = StructType(Array(
      StructField("prdtid", StringType, true),
      StructField("sectype", StringType, true),
      StructField("secsubtype", StringType, true),
      StructField("symbol", StringType, true)
    ))
    val prdDF=hiveContext.createDataFrame(newprd, structType)

    println("2-----------------zk:--------------------"+zkQurom)
    var confh=new org.apache.hadoop.conf.Configuration();
    confh.set("dfs.replication","1")

    val hdfs = org.apache.hadoop.fs.FileSystem.get(new java.net.URI("hdfs://nameservice1"), confh)
    val snapfls: Array[FileStatus] = hdfs.listStatus(new Path("hdfs://nameservice1/user/u010571/mddata/snapshotold"))
    val days=getDayList(hiveContext,hdfs,snapfls)
    val startDay=args(0)
    val endDay=args(1)
    val inserData=args(2)
    for(day <- days){
      if(day >=startDay && day <=endDay){
        val year=day.substring(0,4)
        val mon=day.substring(4,6)
        val daye=day.substring(6)

        println("开始导入"+year+"/"+mon+"/"+daye+"的数据")
        val indexSql="select t.tradedate,t.datatimestamp,concat(t.securityid,'.SZ') as securityid,t.closeindex,t.totalvolumetraded,t.turnover," +
          "  t.lastindex,t.openindex,t.highindex,t.lowindex  from table_app_t_md_index_old t left join " +
          "(select AA.securityid,AA.datatimestamp from(select t.securityid,t.datatimestamp,t.totalvolumetraded, " +
          " lag(t.datatimestamp,1) over (partition by t.securityid order by t.datatimestamp asc,t.totalvolumetraded asc) as lasttime, " +
          "  lag(t.totalvolumetraded,1) over (partition by t.securityid order by t.datatimestamp asc,t.totalvolumetraded asc) as lastvol  " +
          "    from table_app_t_md_index_old t where t.year='"+year+"' and t.month='"+mon+"' and t.day='"+daye+"')" +
          " AA where AA.datatimestamp<AA.lasttime or AA.totalvolumetraded < AA.lastvol) BB " +
          " on t.securityid=BB.securityid and t.datatimestamp=BB.datatimestamp " +
          "  where t.year='"+year+"' and t.month='"+mon+"' and t.day='"+daye+"' and BB.securityid is null and BB.datatimestamp is null "

        //定义 HBase 的配置
      //  val hbaseconf = HBaseConfiguration.create()
      //  hbaseconf.set("hbase.zookeeper.property.clientPort", "2181")
      //  hbaseconf.set("hbase.zookeeper.quorum", zkQurom)
        sc.hadoopConfiguration.set("hbase.zookeeper.quorum","arch-bd-zookeeper2,arch-bd-zookeeper4,arch-bd-zookeeper1,arch-bd-zookeeper3,arch-bd-zookeeper5")
        sc.hadoopConfiguration.set("hbase.zookeeper.property.clientPort", "2181")
        sc.hadoopConfiguration.set(TableOutputFormat.OUTPUT_TABLE, "MDC:MDIndexRecord")
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
        val localData=midStockDF.map(convertIndexType)

        val c11=localData.count()
        if(inserData.equals("T")){
          localData.saveAsNewAPIHadoopDataset(jobConf.getConfiguration)
        }else{
          val sa=midStockDF.head(1)(0)
          val tradeDate = sa.getString(0)
          val format=new SimpleDateFormat("yyyyMMddHHmmssSSS")
          //  val tradeDatetimestamp=sa(1).toString.length==8?("0"+sa(1).toString):sa(1)
          val sec=sa.getString(2)
          var origtime=sa.getLong(1).toString
          if(sa(1).toString.length==8){
            origtime="0"+origtime
          }
          origtime=sa(0)+origtime
          val origt=format.parse(origtime)
          val rowKey:String=MD5Hash.getMD5AsHex(sec.getBytes).substring(0,6)+sec+(java.lang.Long.MAX_VALUE-origt.getTime)
          println("第一个记录：time:"+origtime+" sec:"+sec+" rowKey:"+rowKey)
        }
        //localData.saveAsHadoopDataset(jobConf)

        println("导入"+year+"/"+mon+"/"+daye+"的index指数数据结束,共导入数据:"+c11)

      }
    }
    sc.stop()

  }



  def convertIndexType(row: Row)= {
    val format=new SimpleDateFormat("yyyyMMddHHmmssSSS")
    val secid=row.getAs[String]("securityid")
    val HTSCSecurityID=secid

    val MDDate= row.getAs[String]("tradedate");
    var origtime=row.getAs[Long]("datatimestamp").toString;
    if(origtime.length==8){
      origtime="0"+origtime
    }
    val origtime1=MDDate+origtime
    val origt=format.parse(origtime1)
    val rowKey:String=MD5Hash.getMD5AsHex(HTSCSecurityID.getBytes).substring(0,6)+HTSCSecurityID+(java.lang.Long.MAX_VALUE-origt.getTime)
    val sectype=row.getAs[String]("sectype")
    val subType=row.getAs[String]("secsubtype")
    val symbol=row.getAs[String]("symbol")
    //val mdstreamid=row.getAs[String]("mdstreamid")
    val p = new Put(Bytes.toBytes(rowKey))

    val preclosepx=row.getAs[Double]("closeindex")
    val totalVolumnTrade=row.getAs[Long]("totalvolumetraded")
    val totalValueTrade=row.getAs[Double]("turnover")
    val lastpx=row.getAs[Double]("lastindex")
    val openpx=row.getAs[Double]("openindex")
    val highpx=row.getAs[Double]("highindex")
    val lowpx=row.getAs[Double]("lowindex")

    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("MDDate"), Bytes.toBytes(MDDate))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("MDTime"), Bytes.toBytes(origtime))
   // p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("MDStreamID"), Bytes.toBytes(mdstreamid))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("SecurityType"), Bytes.toBytes(sectype))
    if("-"!=subType) {
      p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("SecuritySubType"), Bytes.toBytes(subType))
    }
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("SecurityID"), Bytes.toBytes(secid.substring(0,secid.length-3)))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("SecurityIDSource"), Bytes.toBytes("102"))
    if("-"!=symbol) {
      p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("Symbol"), Bytes.toBytes(symbol))
    }
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("HTSCSecurityID"), Bytes.toBytes(HTSCSecurityID))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("MDLevel"), Bytes.toBytes("1"))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("MDChannel"), Bytes.toBytes("4"))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("MDRecordType"), Bytes.toBytes("1"))
    //p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("TradingPhaseCode"), Bytes.toBytes(tpc))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("PreClosePx"), Bytes.toBytes(preclosepx.toString))
  //  p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("NumTrades"), Bytes.toBytes(numtrades))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("TotalVolumeTrade"), Bytes.toBytes(totalVolumnTrade.toString))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("TotalValueTrade"), Bytes.toBytes(totalValueTrade.toString))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("LastPx"), Bytes.toBytes(lastpx.toString))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("OpenPx"), Bytes.toBytes(openpx.toString))
    // p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("ClosePx"), Bytes.toBytes())
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("HighPx"), Bytes.toBytes(highpx.toString))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("LowPx"), Bytes.toBytes(lowpx.toString))

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
