package cn.com.htsc.hqcenter.outproperties

import java.text.SimpleDateFormat
import java.util.ResourceBundle

import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.apache.hadoop.hbase.client.{Put, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.hbase.util.{Bytes, MD5Hash}
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{StringType, StructField, StructType}

/**
  *
  * @author
  * @version $Id:
  *
  */
object InsertOrderIntoHbasePrdEnv {
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
    //  Row(row.split(",")(0),row.split(",")(1),row.split(",")(2),row.split(",")(3))
    //}).filter(row=>{row(0).toString.endsWith(".SZ")})
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
    val prdDF=hiveContext.createDataFrame(newprd, structType)//.registerTempTable("prdDF")
    //hiveContext.cacheTable("prdDF")

    val hdfs = org.apache.hadoop.fs.FileSystem.get(new java.net.URI("hdfs://nameservice1"), new org.apache.hadoop.conf.Configuration())

    val snapfls: Array[FileStatus] = hdfs.listStatus(new Path("hdfs://nameservice1/user/u010571/mddata/snapshotnew"))
    val days=getDayList(hiveContext,hdfs,snapfls)
    val startDay=args(0)
    val endDay=args(1)
    val startt=args(2)
    val endt=args(3)
    val insertData=args(4)
    for(day <- days){
      if(day >=startDay && day <=endDay){
        val year=day.substring(0,4)
        val mon=day.substring(4,6)
        val daye=day.substring(6)
       // insertEvertDay(hiveContext,prdDF,year,month,daye,zkQurom)
        println("开始导入"+year+"/"+mon+"/"+daye+"的数据")
        val sstart=year+mon+daye+startt
        val sendt=year+mon+daye+endt

        val indexSql="select t.origtime,t.mdstreamid,t.securityid,t.applseqnum," +
          "case t.ordertype when 'U' then 3" +
          " else cast(t.ordertype as int) " +
          "end as ordertype, t.price, t.orderqty, case t.side when 'G' then 3   when 'F' then 4" +
          "   else cast(t.side as int) end as OrderBSFlag, " +
          "t.expirationtype,t.expirationdays,t.contactor, t.contactinfo,t.confirmid  " +
          "from table_app_t_md_order t where " +
          "t.year='"+year+"' and t.month='"+mon+"' and t.day='"+daye+"' and t.price>=0.0 and t.origtime between '"+sstart+"' and '"+sendt+"'"
        println("开始导入"+year+"/"+mon+"/"+daye+"的数据"+" 起止时间:"+sstart+"--"+sendt+"\n sql:"+indexSql)
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
        println("5----------------------------------count:"+ldDF.count())
        val midStockDF = ldDF.join(prdDF,ldDF("securityid")===prdDF("prdtid"))
      //val midStockDF = hiveContext.sql("select *from table_app_t_md_order t,prdDF q where t.securityid=q.prdtid")
        println("6--------------------------------join-after-count:"+midStockDF.count())
        //  val localData = hiveContext.sql(selectSnapSql).map(convertStockFundType(_,2))
      if(insertData.equals("T")){
        val localData=midStockDF.map(convertOrderType)
      }else{
        val sa=midStockDF.head(1)(0)
        val format=new SimpleDateFormat("yyyyMMddHHmmssSSS")
        val secid=sa.getAs[String]("securityid")
        val HTSCSecurityID=secid+".SZ"
        val origtime=sa.getAs[String]("origtime");
        //val MDDate= sa.getAs[String]("origtime").substring(0,8);
        val origt=format.parse(sa.getAs[String]("origtime"))
        val applseqnum=sa.getAs[Long]("applseqnum")
        val rowKey:String=MD5Hash.getMD5AsHex(HTSCSecurityID.getBytes).substring(0,6)+HTSCSecurityID+
          (java.lang.Long.MAX_VALUE-origt.getTime)+( java.lang.Long.MAX_VALUE-applseqnum);
        println("第一个记录：time:"+origtime+" sec:"+secid+" recno:"+applseqnum+" rowKey:"+rowKey)
      }


        //hiveContext.uncacheTable("prdDF")
        println("导入"+year+"/"+mon+"/"+daye+"的order指数数据结束")

      }
  }
    sc.stop()
  }

  def convertOrderType(row: Row)= {
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

    val orderType=row.getAs[Int]("ordertype")
    val price=row.getAs[Double]("price")
    val orderqty=row.getAs[Long]("orderqty")
    val orderBSFlag=row.getAs[Int]("OrderBSFlag")
    val ExpirationType=row.getAs[Int]("expirationtype")
    val ExpirationDays=row.getAs[Int]("expirationdays")
    val contactor=row.getAs[String]("contactor");
    val contactinfo=row.getAs[String]("contactinfo")
    val confirmid=row.getAs[String]("confirmid")

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
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("MDRecordType"), Bytes.toBytes(4))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("HTSCSecurityID"), Bytes.toBytes(HTSCSecurityID))

    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("OrderIndex"), Bytes.toBytes(applseqnum))
    //  p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("NumTrades"), Bytes.toBytes(numtrades))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("OrderType"), Bytes.toBytes(orderType))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("orderqty"), Bytes.toBytes(orderqty))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("orderBSFlag"), Bytes.toBytes(orderBSFlag))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("ExpirationType"), Bytes.toBytes(ExpirationType))
    // p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("ClosePx"), Bytes.toBytes())
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("ExpirationDays"), Bytes.toBytes(ExpirationDays))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("Contactor"), Bytes.toBytes(contactor))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("ContactInfo"), Bytes.toBytes(contactinfo))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("ConfirmID"), Bytes.toBytes(confirmid))

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
