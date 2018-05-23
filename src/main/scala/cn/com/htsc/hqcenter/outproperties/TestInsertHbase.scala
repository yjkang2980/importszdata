package cn.com.htsc.hqcenter.outproperties

import cn.com.htsc.hqcenter.outproperties.InsertStockFundBondOptionIntoHbase.convertStockFundType
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.sql.Row
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext

/**
  *
  * @author
  * @version $Id:
  *
  */
object TestInsertHbase {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("BusiCompute")
    val sc = new SparkContext(conf)
    val hiveContext = new HiveContext(sc)

    val hbaseconf = HBaseConfiguration.create()
    hbaseconf.set("hbase.zookeeper.property.clientPort", "2181")
    hbaseconf.set("hbase.zookeeper.quorum", "ip-168-61-2-26,ip-168-61-2-27,ip-168-61-2-28")
    println("3-------------------------------------")
    //指定输出格式和输出表名
    val jobConf = new JobConf(hbaseconf, this.getClass)
    jobConf.setOutputFormat(classOf[TableOutputFormat])
    jobConf.set(TableOutputFormat.OUTPUT_TABLE, "test1")
    val prdinfoRdd=sc.textFile("hdfs://nameservice1/user/u010571/data/prdInfo.txt")
    val newprd=prdinfoRdd.map(row=>{
      Row(row.split(",")(0).split("\\.",2)(0),row.split(",")(1),row.split(",")(2),row.split(",")(3))
    })

    val localData=newprd.map(convertType)
    println("6-------------------------------------")
   // println(hiveContext.sql(selectSnapSql).head(1))

    localData.saveAsHadoopDataset(jobConf)


  }
  def convertType(row: Row):(ImmutableBytesWritable,Put) = {
    val p = new Put(Bytes.toBytes(row.getString(0)))
    p.addColumn(Bytes.toBytes("lf"), Bytes.toBytes("col1"), Bytes.toBytes(row.getString(1)))
    (new ImmutableBytesWritable, p)
  }
}
