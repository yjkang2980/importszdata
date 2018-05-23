package cn.com.htsc.hqcenter.dataretrive

import java.text.SimpleDateFormat

import cn.com.htsc.hqcenter.dataretrive.SZIndexDataFileAndHBaseCom.{convertIndexType, insertIndexData}
import cn.com.htsc.hqcenter.dataretrive.SZOrderDataFileAndHBaseCom.insertOrderRec
import cn.com.htsc.hqcenter.dataretrive.SZSnapDataFileAndHBaseCom.insertSnapRec
import cn.com.htsc.hqcenter.dataretrive.SZTradeDataFileAndHBaseCom.insertTradeRec
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 一个参数：代表导入年月日，例如：20180417
  * @author 010571  修正值，从hdfs文件插入差异数据
  * @version $Id:
  *
  */
object ReviseDataFromHDFSFile {

  def retrieveIndex(sc:SparkContext,hiveContext: HiveContext, indexFile: String,ymd:String): Unit = {
      val rowrdd=sc.textFile(indexFile).map(row => {
        Row(row.split(",")(0).substring(1), row.split(",")(1).substring(0,row.split(",")(1).length-1))
      })
    insertIndexData(rowrdd,hiveContext,sc,ymd);
  }

  def retrieveOrder(sc: SparkContext, hiveContext: HiveContext, orderFile: String, str: String) ={
    val rowrdd=sc.textFile(orderFile).map(row => {
      Row(row.split(",")(0).substring(1),row.split(",")(1),row.split(",")(2).substring(0,row.split(",")(2).length-1))
    })
    insertOrderRec(rowrdd,hiveContext,sc,str)
  }

  def retrieveSnap(sc: SparkContext, hiveContext: HiveContext, snapFile: String, str: String) = {
    val rowrdd=sc.textFile(snapFile).map(row => {
      Row(row.split(",")(0).substring(1,row.split(",")(0).length-3),row.split(",")(1).substring(0,row.split(",")(1).length-1))
    })
    insertSnapRec(rowrdd,hiveContext,sc,str)
  }

  def retrieveTrade(sc: SparkContext, hiveContext: HiveContext, tradeFile: String, str: String): Unit = {
    val rowrdd=sc.textFile(tradeFile).map(row => {
      Row(row.split(",")(0).substring(1),row.split(",")(1),row.split(",")(2).substring(0,row.split(",")(2).length-1))
    })
    insertTradeRec(rowrdd,hiveContext,sc,str)
  }

  def main(args: Array[String]): Unit = {

    val hdfs = org.apache.hadoop.fs.FileSystem.get(new java.net.URI("hdfs://nameservice1"), new org.apache.hadoop.conf.Configuration())

    val sdf: SimpleDateFormat = new SimpleDateFormat("yyyyMMddHHmmssSSS")

    val indexFile="hdfs://nameservice1/user/u010571/dataretrieve/"+args(0)+"/index/szHasHbaseNull/part-00000"
    val orderFile="hdfs://nameservice1/user/u010571/dataretrieve/"+args(0)+"/order/szHasHbaseNull/part-00000"
    val snapFile="hdfs://nameservice1/user/u010571/dataretrieve/"+args(0)+"/snaplevel/szHasHbaseNull/part-00000"
    val tradeFile="hdfs://nameservice1/user/u010571/dataretrieve/"+args(0)+"/trade/szHasHbaseNull/part-00000"
    val conf = new SparkConf().setAppName("hqszhistorybulu")
    val sc = new SparkContext(conf)
    val hiveContext = new HiveContext(sc)
    hiveContext.sql("use mdc")
    if(hdfs.exists(new Path(indexFile))){
      retrieveIndex(sc,hiveContext,indexFile,args(0))
    }

    println("完成指数指定日期插入.....,开始order类型")
    if(hdfs.exists(new Path(orderFile))){
      retrieveOrder(sc,hiveContext,orderFile,args(0))
    }

    println("完成逐笔订单插入，...开始snap类型")
    if(hdfs.exists(new Path(snapFile))){
      retrieveSnap(sc,hiveContext,snapFile,args(0))
    }

    println("完成snap，开始trade类型补录插入.....")
    if(hdfs.exists(new Path(tradeFile))){
      retrieveTrade(sc,hiveContext,tradeFile,args(0))
    }

  }

}
