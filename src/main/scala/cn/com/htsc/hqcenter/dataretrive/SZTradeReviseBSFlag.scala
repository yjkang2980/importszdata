package cn.com.htsc.hqcenter.dataretrive

import java.text.SimpleDateFormat

import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.hadoop.hbase.client.{Put, Result, Scan}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.hbase.util.{Bytes, MD5Hash}
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.{SparkConf, SparkContext}

/**
  *修正bsflag字段
  * @author 010571 
  * @version $Id:
  *
  */
object SZTradeReviseBSFlag {
  val hdfs = org.apache.hadoop.fs.FileSystem.get(new java.net.URI("hdfs://nameservice1"), new org.apache.hadoop.conf.Configuration())

  val sdf: SimpleDateFormat = new SimpleDateFormat("yyyyMMddHHmmssSSS")
  var year=sdf.format(new java.util.Date()).substring(0,4);
  var month=sdf.format(new java.util.Date()).substring(4,6);
  var day=sdf.format(new java.util.Date()).substring(6,8);
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

  var insertRec=false

  var resultDir="/user/u010571/dataretrieve/"+year+month+day+"/trade/statistic"
  var dataOutDir="/user/u010571/dataretrieve/"+year+month+day+"/trade/szHasHbaseNull"
  var dataOutDir2="/user/u010571/dataretrieve/"+year+month+day+"/trade/HbaseHasszNull"

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("hqsztradedatacompare")
    val sc = new SparkContext(conf)
    val hiveContext = new HiveContext(sc)
    hiveContext.sql("use mdc")
   //val year="2018"
   // val mon="03"
   // val month="03"
   // val day="20"
    val securityid="000005"
   // val sdf: SimpleDateFormat = new SimpleDateFormat("yyyyMMddHHmmssSSS")
    if(args.length>0){
      year=args(0)
      month=args(1)
      day=args(2)
      val ir=args(3)
      if(ir.equals("T")){
        println("校验后插入数据")
        insertRec=true
      }
    }
     resultDir="/user/u010571/dataretrieve/"+year+month+day+"/trade/statistic"
     dataOutDir="/user/u010571/dataretrieve/"+year+month+day+"/trade/szHasHbaseNull"
     dataOutDir2="/user/u010571/dataretrieve/"+year+month+day+"/trade/HbaseHasszNull"


    val dataPath=new Path(dataOutDir)
    val dataPath2=new Path(dataOutDir2)
    val resultDIr=new Path(resultDir)

    if(hdfs.exists(resultDIr)){
      hdfs.delete(resultDIr,true)
    }

    if(hdfs.exists(dataPath)){
      hdfs.delete(dataPath,true)
    }
    if(hdfs.exists(dataPath2)){
      hdfs.delete(dataPath2,true)
    }

    println("-----------------")

    val result=insertTradeRec(hiveContext,sc,year+month+day);
    if(null!=result){
      sc.makeRDD(Array(result)).coalesce(1, true).saveAsTextFile(resultDir);
      //完成后，上传到ftp
      /*if(hdfs.exists(new Path(resultDIr+"/part-00000"))){
        val in = hdfs.open(new Path(resultDIr+"/part-00000"))
        UPloadToFtp.uploadToFtp(in, "/hjp/"+year+month+day+"/trade/statistic", "part-00000.txt")
      }
      if(hdfs.exists(new Path(dataOutDir+"/part-00000"))){
        val in = hdfs.open(new Path(dataOutDir+"/part-00000"))
        UPloadToFtp.uploadToFtp(in, "/hjp/"+year+month+day+"/trade/szHasHbaseNull", "part-00000.txt")
      }*/
    }


  }

  def insertTradeRec(hiveContext: HiveContext, sc: SparkContext, ymd: String) = {

    val indexData=hiveContext.sql("select *from table_app_t_md_trade t where  t.year='"+ymd.substring(0,4)+"' and t.month='"+ymd.substring(4,6)+"' and t.day='"+ymd.substring(6,8)+"' and t.price!=0.0 and t.tradeqty!=0")

    //[300696,20180330093432150,871663]
    //midStockDF=midStockDF.filter(midStockDF("securityid").equalTo("300696")).filter(midStockDF("origtime").equalTo("20180330093432150")).filter(midStockDF("applseqnum").equalTo("871663"));

    val localData=indexData.map(convertTradeType)
    //midStockDF=midStockDF.filter(midStockDF("securityid").equalTo("000977")).filter(midStockDF("origtime").equalTo("20180330093234590")).filter(midStockDF("applseqnum").equalTo("648070"));
    println("需要补录的数据记录数："+localData.count())
    sc.hadoopConfiguration.set("hbase.zookeeper.quorum","arch-bd-zookeeper2,arch-bd-zookeeper4,arch-bd-zookeeper1,arch-bd-zookeeper3,arch-bd-zookeeper5")
    sc.hadoopConfiguration.set("hbase.zookeeper.property.clientPort", "2181")
    sc.hadoopConfiguration.set(TableOutputFormat.OUTPUT_TABLE, "MDC:MDTransactionRecord")
    //指定输出格式和输出表名
    //val jobConf = new JobConf(hbaseconf, this.getClass)
    val jobConf = new Job(sc.hadoopConfiguration)
    jobConf.setOutputKeyClass(classOf[ImmutableBytesWritable])
    jobConf.setOutputValueClass(classOf[Result])
    jobConf.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])
    localData.saveAsNewAPIHadoopDataset(jobConf.getConfiguration)
  }



  def convertTradeType(row: Row)= {
    val format=new SimpleDateFormat("yyyyMMddHHmmssSSS")
    val secid=row.getAs[String]("securityid")
    val HTSCSecurityID=secid+".SZ"
    val origtime=row.getAs[String]("origtime").substring(8);
    val MDDate= row.getAs[String]("origtime").substring(0,8);
    val origt=format.parse(row.getAs[String]("origtime"))
    val applseqnum=row.getAs[String]("applseqnum")
    val rowKey:String=MD5Hash.getMD5AsHex(HTSCSecurityID.getBytes).substring(0,6)+HTSCSecurityID+
      (java.lang.Long.MAX_VALUE-origt.getTime)+( java.lang.Long.MAX_VALUE-java.lang.Long.parseLong(applseqnum));
    val sectype=row.getAs[String]("sectype")
    val subType=row.getAs[String]("secsubtype")
    val symbol=row.getAs[String]("symbol")
    val mdstreamid=row.getAs[String]("mdstreamid")
    val p = new Put(Bytes.toBytes(rowKey))
    val tradetype=row.getAs[String]("exectype")
    val bidapplseqnum=row.getAs[Long]("bidapplseqnum")
    val offerapplseqnum=row.getAs[Long]("offerapplseqnum")

    if(bidapplseqnum>offerapplseqnum){
      p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("TradeBSFlag"), Bytes.toBytes("1"))
    }
    if(bidapplseqnum<offerapplseqnum){
      p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("TradeBSFlag"), Bytes.toBytes("2"))
    }


    (new ImmutableBytesWritable, p)
  }

}
