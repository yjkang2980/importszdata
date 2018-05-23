package cn.com.htsc.hqcenter.outproperties

import java.util.ResourceBundle

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}

/**
  *
  * @author
  * @version $Id:

  *
  */
object NewFormatImportDeal {
  def main(args: Array[String]): Unit = {




    val conf = new SparkConf().setAppName("hqdataimport")
    val sc = new SparkContext(conf)
    val hiveContext = new HiveContext(sc)
    val sqlContext = new SQLContext(sc)

    val dataraw = sc.textFile("hdfs://nameservice1/user/u010571/snapshotnew/2016/08/31/am_hq_snap_spot.txt")
   // sqlContext.



    val rddrow = dataraw.map(x=>Row(
         x.split("\t")(0),x.split("\t")(1),x.split("\t")(2),x.split("\t")(3),
         x.split("\t")(4),x.split("\t")(5),x.split("\t")(6),x.split("\t")(7),
         x.split("\t")(8),x.split("\t")(9),x.split("\t")(10),x.split("\t")(11),
         x.split("\t")(13),x.split("\t")(14),x.split("\t")(17),x.split("\t")(19)
       ))
    val structType = StructType(Array(
      StructField("s1", StringType, true),
      StructField("s2", StringType, true),
      StructField("s3", StringType, true),
      StructField("s4", StringType, true),
      StructField("s5", StringType, true),
      StructField("s6", StringType, true),
      StructField("s7", StringType, true),
      StructField("s8", StringType, true),
      StructField("s9", StringType, true),
      StructField("s10", StringType, true),
      StructField("s11", StringType, true),
      StructField("s12", StringType, true),
      StructField("s13", StringType, true),
      StructField("s14", StringType, true),
      StructField("s15", StringType, true),
      StructField("s16", StringType, true)
    ))


    var df = sqlContext.createDataFrame(rddrow,structType)
    df.head(5).foreach(x=>println(x))
    //求和
    df.filter("s12 > 7000000.00").agg("s12"->"sum")
    val s2 = df.filter("s12 <= 7000000.00").agg("s12"->"sum")
    sqlContext.sql("use ana_crmpicture")
    val data1=sqlContext.sql("select *from table_app_t_md_trade t limit 100")
      //data1.s

    print(s2.toString())

  }
}
