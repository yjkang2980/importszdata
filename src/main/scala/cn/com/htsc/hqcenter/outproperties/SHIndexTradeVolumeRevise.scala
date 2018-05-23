package cn.com.htsc.hqcenter.outproperties

import java.io.{BufferedReader, FileReader}
import java.util

import org.apache.hadoop.hbase.client.{Result, Scan}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.util.Base64
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.sql.Row
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}

import scala.io.Source
import scala.util.control.Breaks._


/**
  *
  * @author
  * @version $Id:
  *
  */
object SHIndexTradeVolumeRevise {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("hqdataimport")
    val sc = new SparkContext(sparkConf)

    val prdinfoRdd = sc.textFile("hdfs://nameservice1/user/u010571/data/prdInfo.txt")

    val br = new BufferedReader(new FileReader("prdInfo.txt"))
    var line: String = null

    val file=Source.fromFile("prdInfo.txt")
    for(line <- file.getLines)
    {
      val prdid = line.split(",")(0)
      val ty:String = line.split(",")(1)
      //println("开始"+prdid+" 的校验"+" ty:"+ty)
        if (ty.equals("2")) {
          println("开始"+prdid+" 的校验"+" ty:"+ty)
           getData(sc,prdid)
        }
    }
    file.close


    while ((line = br.readLine()) != null) {
      val prdid = line.split(",")(0)
      val ty = line.split(",")(1)
      breakable {

        if (!ty.contentEquals("2")) {
           break
        }
      }
      println("开始"+prdid+" 的校验")
      getData(sc,prdid)
      println("-----------------------------完成id:"+prdid+"全年检查---------------------------------------")
    }

    //val newprd = prdinfoRdd.map(row => {
    // Row(row.split(",")(0), row.split(",")(1), row.split(",")(2), row.split(",")(3))

    val structType = StructType(Array(
      StructField("prdtid", StringType, true),
      StructField("sectype", StringType, true),
      StructField("secsubtype", StringType, true),
      StructField("symbol", StringType, true)
    ))


    val hiveContext = new HiveContext(sc)
    hiveContext.sql("use mdc")
    // val prdDF = hiveContext.createDataFrame(newprd, structType)

    //  prdDF.map(checkData)


    //val hBaseRDD = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat],
    //  classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
    //  classOf[org.apache.hadoop.hbase.client.Result])
  }


  def getData(sc: SparkContext, prdid: String) = {
    val cols = List[String]("Buy1OrderQty", "Buy1Price", "Buy1NumOrders", "Buy1NoOrders", "Buy2OrderQty", "Buy2Price", "Buy2NumOrders", "Buy2NoOrders", "Buy3OrderQty", "Buy3Price", "Buy3NumOrders", "Buy3NoOrders", "Buy4OrderQty", "Buy4Price", "Buy4NumOrders", "Buy4NoOrders", "Buy5OrderQty", "Buy5Price", "Buy5NumOrders", "Buy5NoOrders", "Buy6OrderQty", "Buy6Price", "Buy6NumOrders", "Buy6NoOrders", "Buy7OrderQty", "Buy7Price", "Buy7NumOrders", "Buy7NoOrders", "Buy8OrderQty", "Buy8Price", "Buy8NumOrders", "Buy8NoOrders", "Buy8OrderQty", "Buy8Price", "Buy8NumOrders", "Buy8NoOrders", "Buy10OrderQty", "Buy10Price", "Buy10NumOrders", "Buy10NoOrders", "Sell1OrderQty", "Sell1Price", "Sell1NumOrders", "Sell1NoOrders", "Sell2OrderQty", "Sell2Price", "Sell2NumOrders", "Sell2NoOrders", "Sell3OrderQty", "Sell3Price", "Sell3NumOrders", "Sell3NoOrders", "Sell4OrderQty", "Sell4Price", "Sell4NumOrders", "Sell4NoOrders", "Sell5OrderQty", "Sell5Price", "Sell5NumOrders", "Sell5NoOrders", "Sell6OrderQty", "Sell6Price", "Sell6NumOrders", "Sell6NoOrders", "Sell7OrderQty", "Sell7Price", "Sell7NumOrders", "Sell8NoOrders", "Sell8OrderQty", "Sell8Price", "Sell8NumOrders", "Sell8NoOrders", "Sell9OrderQty", "Sell9Price", "Sell9NumOrders", "Sell9NoOrders", "Sell10OrderQty", "Sell10Price", "Sell10NumOrders", "Sell10NoOrders", "PreClosePx", "NumTrades", "TotalVolumeTrade", "TotalValueTrade", "LastPx", "OpenPx", "ClosePx", "HighPx", "LowPx", "DiffPx1", "DiffPx2", "MinPx", "MaxPx", "TotalBidQty", "TotalOfferQty", "WeightedAvgBidPx", "WeightedAvgOfferPx", "WithdrawBuyNumber", "WithdrawBuyAmount", "WithdrawBuyMoney", "WithdrawSellNumber", "WithdrawSellAmount", "WithdrawSellMoney", "TotalBidNumber", "TotalOfferNumber", "BidTradeMaxDuration", "OfferTradeMaxDuration", "NumBidOrders", "NumOfferOrders", "SLYOne", "SLYTwo")
    //val colss:List[Array[String]] = util.Arrays.asList(cols)
    sc.broadcast(cols)

    //var prdid="601688.SH"
    sc.hadoopConfiguration.set("hbase.zookeeper.quorum", "arch-bd-zookeeper2,arch-bd-zookeeper4,arch-bd-zookeeper1,arch-bd-zookeeper3,arch-bd-zookeeper5")
    sc.hadoopConfiguration.set("hbase.zookeeper.property.clientPort", "2181")

   // sc.hadoopConfiguration.set("hbase.zookeeper.quorum", "ip-168-61-2-26,ip-168-61-2-27,ip-168-61-2-28")
   // sc.hadoopConfiguration.set("hbase.zookeeper.property.clientPort", "2181")

    // sc.hadoopConfiguration.set(TableOutputFormat.OUTPUT_TABLE, "MDC:MDIndexRecord")
    sc.hadoopConfiguration.set(TableInputFormat.INPUT_TABLE, "MDC:MDStockRecord")
    //指定输出格式和输出表名
    //val jobConf = new JobConf(hbaseconf, this.getClass)
    //val jobConf = new Job(sc.hadoopConfiguration)
    //jobConf.setOutputKeyClass(classOf[ImmutableBytesWritable])
    // jobConf.setOutputValueClass(classOf[Result])
    // jobConf.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])
    val scan = new Scan()
    import org.apache.hadoop.hbase.util.Bytes
    import org.apache.hadoop.hbase.util.MD5Hash
    import java.text.SimpleDateFormat
    val format = new SimpleDateFormat("yyyyMMddHHmmssSSS")
    val end = java.lang.Long.MAX_VALUE - format.parse("20170103" + "080000000").getTime
    val start = java.lang.Long.MAX_VALUE - format.parse("20180103" + "173100000").getTime
    val startrowKey = MD5Hash.getMD5AsHex(prdid.getBytes).substring(0, 6) + prdid + start
    val endKey = MD5Hash.getMD5AsHex(prdid.getBytes).substring(0, 6) + prdid + end

    scan.setStartRow(Bytes.toBytes(startrowKey))
    scan.setStopRow(Bytes.toBytes(endKey))

    val proto = ProtobufUtil.toScan(scan)

    sc.hadoopConfiguration.set(TableInputFormat.SCAN, Base64.encodeBytes(proto.toByteArray))

    val usersRDD = sc.newAPIHadoopRDD(sc.hadoopConfiguration, classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])

    val nu2=usersRDD.map(row=>row._2).filter(r=>{
      import org.apache.hadoop.hbase.util.Bytes
        val row = new String(r.getRow)
      var times = row.substring(15)
      if (prdid.endsWith("HK")) times = row.substring(14)
      // LOGGER.info(secid + "-" + row + "--" + times + "-dd");
      val mdtime = format.format(new java.util.Date(java.lang.Long.MAX_VALUE - java.lang.Long.parseLong(times)))
      val ceList = r.listCells
      import scala.collection.JavaConversions._
      for (cell <- ceList) { //String row = Bytes.toString(cell.getRowArray(), cell.getRowOffset(),
        //       cell.getRowLength());
        val value = Bytes.toString(cell.getValueArray, cell.getValueOffset)
        //空值列必须指定列名,mdstream这样的列很多为空
        if (null==value|| value.eq("") || value == "NaN") {
          val quali = Bytes.toString(cell.getQualifierArray, cell.getQualifierOffset, cell.getQualifierLength)
          if (cols.contains(quali)) {
            val recs = Array(prdid, row, mdtime, quali, value)
            true
          }

        }
      }
      import org.apache.hadoop.hbase.util.Bytes
      val buy1p = r.getValue(Bytes.toBytes("Detail"), Bytes.toBytes("Buy1Price"))
      val buy1qtya = r.getValue(Bytes.toBytes("Detail"), Bytes.toBytes("Buy1OrderQty"))

      val buy2p = r.getValue(Bytes.toBytes("Detail"), Bytes.toBytes("Buy2Price"))
      val buy2qtya = r.getValue(Bytes.toBytes("Detail"), Bytes.toBytes("Buy2OrderQty"))

      import org.apache.hadoop.hbase.util.Bytes

      val buy3p = r.getValue(Bytes.toBytes("Detail"), Bytes.toBytes("Buy3Price"))
      val buy3qtya = r.getValue(Bytes.toBytes("Detail"), Bytes.toBytes("Buy3OrderQty"))

      val buy4p = r.getValue(Bytes.toBytes("Detail"), Bytes.toBytes("Buy4Price"))
      val buy4qtya = r.getValue(Bytes.toBytes("Detail"), Bytes.toBytes("Buy4OrderQty"))

      val buy5p = r.getValue(Bytes.toBytes("Detail"), Bytes.toBytes("Buy5Price"))
      val buy5qtya = r.getValue(Bytes.toBytes("Detail"), Bytes.toBytes("Buy5OrderQty"))

      val buy6p = r.getValue(Bytes.toBytes("Detail"), Bytes.toBytes("Buy6Price"))
      val buy6qtya = r.getValue(Bytes.toBytes("Detail"), Bytes.toBytes("Buy6OrderQty"))

      val buy7p = r.getValue(Bytes.toBytes("Detail"), Bytes.toBytes("Buy7Price"))
      val buy7qtya = r.getValue(Bytes.toBytes("Detail"), Bytes.toBytes("Buy7OrderQty"))

      val buy8p = r.getValue(Bytes.toBytes("Detail"), Bytes.toBytes("Buy8Price"))
      val buy8qtya = r.getValue(Bytes.toBytes("Detail"), Bytes.toBytes("Buy8OrderQty"))

      val buy9p = r.getValue(Bytes.toBytes("Detail"), Bytes.toBytes("Buy9Price"))
      val buy9qtya = r.getValue(Bytes.toBytes("Detail"), Bytes.toBytes("Buy9OrderQty"))

      val buy10p = r.getValue(Bytes.toBytes("Detail"), Bytes.toBytes("Buy10Price"))
      val buy10qtya = r.getValue(Bytes.toBytes("Detail"), Bytes.toBytes("Buy10OrderQty"))

      val sell1p = r.getValue(Bytes.toBytes("Detail"), Bytes.toBytes("Sell1Price"))
      val sell1qtya = r.getValue(Bytes.toBytes("Detail"), Bytes.toBytes("Sell1OrderQty"))

      val sell2p = r.getValue(Bytes.toBytes("Detail"), Bytes.toBytes("Sell2Price"))
      val sell2qtya = r.getValue(Bytes.toBytes("Detail"), Bytes.toBytes("Sell2OrderQty"))

      val sell3p = r.getValue(Bytes.toBytes("Detail"), Bytes.toBytes("Sell3Price"))
      val sell3qtya = r.getValue(Bytes.toBytes("Detail"), Bytes.toBytes("Sell3OrderQty"))

      val sell4p = r.getValue(Bytes.toBytes("Detail"), Bytes.toBytes("Sell4Price"))
      val sell4qtya = r.getValue(Bytes.toBytes("Detail"), Bytes.toBytes("Sell4OrderQty"))

      val sell5p = r.getValue(Bytes.toBytes("Detail"), Bytes.toBytes("Sell5Price"))
      val sell5qtya = r.getValue(Bytes.toBytes("Detail"), Bytes.toBytes("Sell5OrderQty"))

      val sell6p = r.getValue(Bytes.toBytes("Detail"), Bytes.toBytes("Sell6Price"))
      val sell6qtya = r.getValue(Bytes.toBytes("Detail"), Bytes.toBytes("Sell6OrderQty"))

      val sell7p = r.getValue(Bytes.toBytes("Detail"), Bytes.toBytes("Sell7Price"))
      val sell7qtya = r.getValue(Bytes.toBytes("Detail"), Bytes.toBytes("Sell7OrderQty"))

      val sell8p = r.getValue(Bytes.toBytes("Detail"), Bytes.toBytes("Sell8Price"))
      val sell8qtya = r.getValue(Bytes.toBytes("Detail"), Bytes.toBytes("Sell8OrderQty"))

      val sell9p = r.getValue(Bytes.toBytes("Detail"), Bytes.toBytes("Sell9Price"))
      val sell9qtya = r.getValue(Bytes.toBytes("Detail"), Bytes.toBytes("Sell9OrderQty"))

      val sell10p = r.getValue(Bytes.toBytes("Detail"), Bytes.toBytes("Sell10Price"))
      val sell10qtya = r.getValue(Bytes.toBytes("Detail"), Bytes.toBytes("Sell10OrderQty"))

      if (buy1p != null && buy1qtya != null && buy2p != null && buy2qtya != null && buy3p != null && buy3qtya != null && buy4p != null && buy4qtya != null && buy5p != null && buy5qtya != null && buy6p != null && buy6qtya != null && buy7p != null && buy7qtya != null && buy8p != null && buy8qtya != null && buy9p != null && buy9qtya != null && buy10p != null && buy10qtya != null && sell1p != null && sell1qtya != null && sell2p != null && sell2qtya != null && sell3p != null && sell3qtya != null && sell4p != null && sell4qtya != null && sell5p != null && sell5qtya != null && sell6p != null && sell6qtya != null && sell7p != null && sell7qtya != null && sell8p != null && sell8qtya != null && sell9p != null && sell9qtya != null && sell10p != null && sell10qtya != null) {
        val buy1px = new String(buy1p)
        val buy1qty = new String(buy1qtya)
        val buy2px = new String(buy2p)
        val buy2qty = new String(buy2qtya)
        val buy3px = new String(buy3p)
        val buy3qty = new String(buy3qtya)
        val buy4px = new String(buy4p)
        val buy4qty = new String(buy4qtya)
        val buy5px = new String(buy5p)
        val buy5qty = new String(buy5qtya)
        val buy6px = new String(buy6p)
        val buy6qty = new String(buy6qtya)
        val buy7px = new String(buy7p)
        val buy7qty = new String(buy7qtya)
        val buy8px = new String(buy8p)
        val buy8qty = new String(buy8qtya)
        val buy9px = new String(buy9p)
        val buy9qty = new String(buy9qtya)
        val buy10px = new String(buy10p)
        val buy10qty = new String(buy10qtya)
        val sell1px = new String(sell1p)
        val sell1qty = new String(sell1qtya)
        val sell2px = new String(sell2p)
        val sell2qty = new String(sell2qtya)
        val sell3px = new String(sell3p)
        val sell3qty = new String(sell3qtya)
        val sell4px = new String(sell4p)
        val sell4qty = new String(sell4qtya)
        val sell5px = new String(sell5p)
        val sell5qty = new String(sell5qtya)
        val sell6px = new String(sell6p)
        val sell6qty = new String(sell6qtya)
        val sell7px = new String(sell7p)
        val sell7qty = new String(sell7qtya)
        val sell8px = new String(sell8p)
        val sell8qty = new String(sell8qtya)
        val sell9px = new String(sell9p)
        val sell9qty = new String(sell9qtya)
        val sell10px = new String(sell10p)
        val sell10qty = new String(sell10qtya)
        if (buy1px == "0" && buy1qty.substring(0, 1).compareTo("0") > 0 || buy2px == "0" && buy2qty.substring(0, 1).compareTo("0") > 0 || buy3px == "0" && buy3qty.substring(0, 1).compareTo("0") > 0 || buy4px == "0" && buy4qty.substring(0, 1).compareTo("0") > 0 || buy5px == "0" && buy5qty.substring(0, 1).compareTo("0") > 0 || buy6px == "0" && buy6qty.substring(0, 1).compareTo("0") > 0 || buy7px == "0" && buy7qty.substring(0, 1).compareTo("0") > 0 || buy8px == "0" && buy8qty.substring(0, 1).compareTo("0") > 0 || buy9px == "0" && buy9qty.substring(0, 1).compareTo("0") > 0 || buy10px == "0" && buy10qty.substring(0, 1).compareTo("0") > 0 || sell1px == "0" && sell1qty.substring(0, 1).compareTo("0") > 0 || sell2px == "0" && sell2qty.substring(0, 1).compareTo("0") > 0 || sell3px == "0" && sell3qty.substring(0, 1).compareTo("0") > 0 || sell4px == "0" && sell4qty.substring(0, 1).compareTo("0") > 0 || sell5px == "0" && sell5qty.substring(0, 1).compareTo("0") > 0 || sell6px == "0" && sell6qty.substring(0, 1).compareTo("0") > 0 || sell7px == "0" && sell7qty.substring(0, 1).compareTo("0") > 0 || sell8px == "0" && sell8qty.substring(0, 1).compareTo("0") > 0 || sell9px == "0" && sell9qty.substring(0, 1).compareTo("0") > 0 || sell10px == "0" && sell10qty.substring(0, 1).compareTo("0") > 0) {
         true
        }
      }
      false;
    })
    val nuc=nu2.count();
    if(nuc>0){
      val res=nu2.map(r=>{
        new String(r.getRow)
      }).saveAsTextFile("hdfs://nameservice1/user/u010571/data/"+prdid+"/")

    }
    println("完成:"+prdid+" 的校验,异常记录:"+nuc)

  }


}
