package cn.com.htsc.hqcenter.outproperties

import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}

/**
  *

  *
  */
object ImportDataSn {


  def main(args: Array[String]): Unit = {


    val conf = new SparkConf().setAppName("hqdataimport")
    val sc = new SparkContext(conf)
    val hiveContext = new HiveContext(sc)
    val sqlContext = new SQLContext(sc)

    val dataraw = sc.textFile("hdfs://nameservice1/user/u010571/snapshotnew/2016/08/31/am_hq_snap_spot.txt")


       val rddrow = dataraw.map(x=>Row(
         x.split("\t")(0),x.split("\t")(1),x.split("\t")(2),x.split("\t")(3),
         x.split("\t")(4).toDouble,x.split("\t")(5).toDouble,x.split("\t")(6).toDouble,x.split("\t")(7).toDouble,
         x.split("\t")(8).toDouble,x.split("\t")(9),x.split("\t")(10),x.split("\t")(11).toDouble,
         x.split("\t")(13).toDouble,x.split("\t")(14).toDouble,x.split("\t")(17).toDouble,x.split("\t")(19).toDouble
       ))
    val structType = StructType(Array(
      StructField("s1", StringType, true),
      StructField("s2", StringType, true),
      StructField("s3", StringType, true),
      StructField("s4", StringType, true),
      StructField("s5", DoubleType, true),
      StructField("s6", DoubleType, true),
      StructField("s7", DoubleType, true),
      StructField("s8", DoubleType, true),
      StructField("s9", DoubleType, true),
      StructField("s10", StringType, true),
      StructField("s11", StringType, true),
      StructField("s12", DoubleType, true),
      StructField("s13", DoubleType, true),
      StructField("s14", DoubleType, true),
      StructField("s15", DoubleType, true),
      StructField("s16", DoubleType, true)
    ))


    var df = sqlContext.createDataFrame(rddrow,structType)
    df.head(5)
    //求和
    df.filter("c12 > 7000000.00").agg("c12"->"sum")
    val s2 = df.filter("s12 <= 7000000.00").agg("s12"->"sum")
    print(s2.toString())

  }




}
