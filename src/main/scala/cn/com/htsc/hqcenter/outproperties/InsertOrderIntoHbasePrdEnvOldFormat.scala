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
object InsertOrderIntoHbasePrdEnvOldFormat {
  //输入参数： 20150105 20150101 F  //起止导入时间包含时间点，是否导入数据，否的话，值是看计算过程统计记录数用以查看数据是否正确
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

   // val d2=prdDF.map(row=>{
   //   Row(row.getString(0).split("\\.",2)(0),row(1),row(2),row(3))
   // })

    var confh=new org.apache.hadoop.conf.Configuration();
    confh.set("dfs.replication","1")

    val hdfs = org.apache.hadoop.fs.FileSystem.get(new java.net.URI("hdfs://nameservice1"), confh)

    val snapfls: Array[FileStatus] = hdfs.listStatus(new Path("hdfs://nameservice1/user/u010571/mddata/snapshotold"))
    val days=getDayList(hiveContext,hdfs,snapfls)
    val startDay=args(0)
    val endDay=args(1)
    val insertData=args(2)
    //val startt=args(2)
    //val endt=args(3)
    for(day <- days){
      if(day >=startDay && day <=endDay){
        val year=day.substring(0,4)
        val mon=day.substring(4,6)
        val daye=day.substring(6)
       // insertEvertDay(hiveContext,prdDF,year,month,daye,zkQurom)
        println("开始导入"+year+"/"+mon+"/"+daye+"的数据")
       // val sstart=year+mon+daye+startt
      //  val sendt=year+mon+daye+endt

        val indexSql="select t.tradedate,t.orderentrytime,concat(t.securityid,'.SZ') as securityid,t.recno,  t.price,t.orderqty,  trim(t.functioncode) " +
          "      as orderbsflag,trim(t.orderkind) as ordertype  from table_app_t_md_order_old t left join " +
          "  (select *from (select AA.securityid,AA.recno,AA.lastrecno ,AA.orderentrytime,AA.lastrectime,(AA.orderentrytime-AA.lastrectime) as difft " +
          "    from(select t.securityid,t.orderentrytime,t.recno," +
          "     lag(t.orderentrytime,1) over(partition by t.securityid order by t.recno asc) as lastrectime," +
          "       lag(t.recno,1) over(partition by t.securityid order by t.recno asc) as lastrecno" +
          "     from table_app_t_md_order_old t where t.year='"+year+"' and t.month='"+mon+"' and t.day='"+daye+"') AA " +
          "    ) BB where  BB.recno<BB.lastrecno or BB.difft<-1000) CC  on t.securityid=CC.securityid and t.recno=CC.recno and " +
          "t.orderentrytime=CC.orderentrytime where t.year='"+year+"' and t.month='"+mon+"' and t.day='"+daye+"' and t.price>=0.0 and t.orderqty>=0 " +
          "   and CC.securityid is null and CC.recno is null and CC.orderentrytime is null"
        println("开始导入"+year+"/"+mon+"/"+daye+"的数据"+" 起止时间:"+startDay+"--"+endDay+"\n sql:"+indexSql)
        //定义 HBase 的配置
        sc.hadoopConfiguration.set("hbase.zookeeper.quorum","arch-bd-zookeeper2,arch-bd-zookeeper4,arch-bd-zookeeper1,arch-bd-zookeeper3,arch-bd-zookeeper5")
        sc.hadoopConfiguration.set("hbase.zookeeper.property.clientPort", "2181")
        sc.hadoopConfiguration.set(TableOutputFormat.OUTPUT_TABLE, "MDC:MDOrderRecord")
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
       // println("5--------------------------可导入的order数量count:"+ldDF.count())
        val midStockDF = ldDF.join(prdDF,ldDF("securityid")===prdDF("prdtid"))

        //  val localData = hiveContext.sql(selectSnapSql).map(convertStockFundType(_,2))
        val localData=midStockDF.map(convertIndexType)

        var c=localData.count()


       if(insertData.equals("T")){
         localData.saveAsNewAPIHadoopDataset(jobConf.getConfiguration)
       }else{
         //println("5--------------------------可导入的order数量count:"+ldDF.count())
         var c2=midStockDF.count()
         println("6----映射hbase put后的数量:"+c+"----------------------------join-after-count:"+c2)
         val sa=midStockDF.head(1)(0)
         val tradeDate = sa.getString(0)
         val format=new SimpleDateFormat("yyyyMMddHHmmssSSS")
         //  val tradeDatetimestamp=sa(1).toString.length==8?("0"+sa(1).toString):sa(1)
         val sec=sa.getString(2)
         val recno=sa.getLong(3)
         var origtime=sa.getLong(1).toString
         if(sa(1).toString.length==8){
           origtime="0"+origtime
         }
         origtime=sa(0)+origtime
         val origt=format.parse(origtime)
         val rowKey:String=MD5Hash.getMD5AsHex(sec.getBytes).substring(0,6)+sec+
           (java.lang.Long.MAX_VALUE-origt.getTime)+(java.lang.Long.MAX_VALUE-recno);
         println("第一个记录：time:"+origtime+" sec:"+sec+" recno:"+recno+" rowKey:"+rowKey)
       }

      //  hiveContext.uncacheTable("prdDF")
        println("导入"+year+"/"+mon+"/"+daye+"的order指数数据结束,导入数据条数:"+c)

      }
  }
   // sc.stop()
  }

  def convertIndexType(row: Row)= {
    val format=new SimpleDateFormat("yyyyMMddHHmmssSSS")
    val secid=row.getAs[String]("securityid")
    val HTSCSecurityID=secid

    val MDDate= row.getAs[String]("tradedate");
    var origtime=row.getAs[Long]("orderentrytime").toString;
    if(origtime.length==8){
         origtime="0"+origtime
    }
    val origtime1=MDDate+origtime

    val origt=format.parse(origtime1)
    val applseqnum=row.getAs[Long]("recno")
    val rowKey:String=MD5Hash.getMD5AsHex(HTSCSecurityID.getBytes).substring(0,6)+HTSCSecurityID+
      (java.lang.Long.MAX_VALUE-origt.getTime)+(java.lang.Long.MAX_VALUE-applseqnum);
    val sectype=row.getAs[String]("sectype")
    val subType=row.getAs[String]("secsubtype")
    val symbol=row.getAs[String]("symbol")
    //val mdstreamid=row.getAs[String]("mdstreamid")
    val p = new Put(Bytes.toBytes(rowKey))

     // val orderType=row.getAs[Int]("ordertype")
    val price=row.getAs[Double]("price")
    val orderqty=row.getAs[Long]("orderqty")
    val orderBSFlag=row.getAs[String]("orderbsflag")
    val ordertype=row.getAs[String]("ordertype")

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
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("MDLevel"), Bytes.toBytes("1"))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("MDChannel"), Bytes.toBytes("4"))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("MDRecordType"), Bytes.toBytes("4"))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("HTSCSecurityID"), Bytes.toBytes(HTSCSecurityID))

    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("OrderIndex"), Bytes.toBytes(applseqnum.toString))
    //  p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("NumTrades"), Bytes.toBytes(numtrades))
   // p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("OrderType"), Bytes.toBytes(orderType))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("OrderQty"), Bytes.toBytes(orderqty.toString))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("OrderPrice"), Bytes.toBytes(price.toString))
    if(orderBSFlag.equals("B")) {
      p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("OrderBSFlag"), Bytes.toBytes("1"))
    }else if(orderBSFlag.equals("C")){
      p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("OrderBSFlag"), Bytes.toBytes("5"))
    }else if(orderBSFlag.equals("S")){
      p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("OrderBSFlag"), Bytes.toBytes("2"))
    }

    if(ordertype.equals("0")){
      p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("OrderType"), Bytes.toBytes("2"))
    }else if(ordertype.equals("X")){
      p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("OrderType"), Bytes.toBytes("3"))
    }else if(ordertype.equals("Y")){
      p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("OrderType"), Bytes.toBytes("4"))
    } else if(ordertype.equals("2")){
      p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("OrderType"), Bytes.toBytes("5"))
    }else  if(ordertype.equals("V")){
      p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("OrderType"), Bytes.toBytes("6"))
    }else if(ordertype.equals("W")){
      p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("OrderType"), Bytes.toBytes("7"))
    }

  //  p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("ExpirationType"), Bytes.toBytes(ExpirationType))
    // p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("ClosePx"), Bytes.toBytes())
   // p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("ExpirationDays"), Bytes.toBytes(ExpirationDays))
   // p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("Contactor"), Bytes.toBytes(contactor))
   // p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("ContactInfo"), Bytes.toBytes(contactinfo))
   // p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("ConfirmID"), Bytes.toBytes(confirmid))

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
