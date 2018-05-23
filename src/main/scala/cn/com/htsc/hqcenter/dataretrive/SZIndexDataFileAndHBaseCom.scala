package cn.com.htsc.hqcenter.dataretrive

import java.io.InputStream
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
  *用spark对指数数据文件进行校验和缺失导入，缺失导入类四个参数分别表示：年、月、日、是否在校验完成后将缺失数据入库,T表示入库
  * @author 010571 
  * @version $Id:
  *
  */
object SZIndexDataFileAndHBaseCom {
  val hdfs = org.apache.hadoop.fs.FileSystem.get(new java.net.URI("hdfs://nameservice1"), new org.apache.hadoop.conf.Configuration())
  //val sdf: SimpleDateFormat = new SimpleDateFormat("yyyyMMddHHmmssSSS")
  val sdf: SimpleDateFormat = new SimpleDateFormat("yyyyMMddHHmmssSSS")
  //val resultDir="/user/u010571/dataretrieve/index/statistic"
  //val dataOutDir="/user/u010571/dataretrieve/index/szHasHbaseNull"
  //val dataOutDir2="/user/u010571/dataretrieve/index/HbaseHasszNull"
  var year=sdf.format(new java.util.Date()).substring(0,4);
  //var mon=sdf.format(new java.util.Date()).substring(4,6);
  var month=sdf.format(new java.util.Date()).substring(4,6);
  var day=sdf.format(new java.util.Date()).substring(6,8);
  var resultDir="/user/u010571/dataretrieve/"+year+month+day+"/index/statistic"
  var dataOutDir="/user/u010571/dataretrieve/"+year+month+day+"/index/szHasHbaseNull"
  var dataOutDir2="/user/u010571/dataretrieve/"+year+month+day+"/index/HbaseHasszNull"

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

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("hqszindexdatacompare")
    val sc = new SparkContext(conf)
    val hiveContext = new HiveContext(sc)
    hiveContext.sql("use mdc")
    //val year="2018"
    //val mon="03"
    //val month="03"
    //val day="20"
    //val securityid="399001"

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
     resultDir="/user/u010571/dataretrieve/"+year+month+day+"/index/statistic"
    dataOutDir="/user/u010571/dataretrieve/"+year+month+day+"/index/szHasHbaseNull"
    dataOutDir2="/user/u010571/dataretrieve/"+year+month+day+"/index/HbaseHasszNull"


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


    val result=checkIndex(sc,hiveContext,year,month,day);
    if(null!=result){
      sc.makeRDD(Array(result)).coalesce(1, true).saveAsTextFile(resultDir);

      //完成后，上传到ftp
      /*if(hdfs.exists(new Path(resultDIr+"/part-00000"))){
        val in = hdfs.open(new Path(resultDIr+"/part-00000"))
        UPloadToFtp.uploadToFtp(in, "/hjp/"+year+month+day+"/index/statistic", "part-00000.txt")
      }
      if(hdfs.exists(new Path(dataOutDir+"/part-00000"))){
        val in = hdfs.open(new Path(dataOutDir+"/part-00000"))
        UPloadToFtp.uploadToFtp(in, "/hjp/"+year+month+day+"/index/szHasHbaseNull", "part-00000.txt")
      }*/
    }
  }

  def insertIndexData(sub: RDD[Row], hiveContext: HiveContext, sc: SparkContext,ymd:String) = {
    val structType = StructType(Array(
      StructField("securityid", StringType, true),
      StructField("origtime", StringType, true)
    ))
    val intersect=hiveContext.createDataFrame(sub, structType)
    val indexData=hiveContext.sql("select *from table_app_t_md_index t where  t.year='"+ymd.substring(0,4)+"' and t.month='"+ymd.substring(4,6)+"' and t.day='"+ymd.substring(6,8)+"'")
    // prdDF.cache();
    val insertToData=intersect.join(indexData, Seq("securityid", "origtime"), "inner")
    val prdinfoRdd=sc.textFile("hdfs://nameservice1/user/u010571/data/prdInfo.txt")

    //val newprd=prdinfoRdd.map(row=>{
    //Row(row.split(",")(0).split("\\.",2)(0),row.split(",")(1),row.split(",")(2),row.split(",")(3))
    //})
    val newprd = prdinfoRdd.map(row => {
      Row(row.split(",")(0), row.split(",")(1), row.split(",")(2), row.split(",")(3))
    }).filter(row => {
      row(0).toString.endsWith(".SZ")
    }).map(row => {
      Row(row(0).toString.split("\\.", 2)(0), row(1), row(2), row(3))
    })

    val structType2 = StructType(Array(
      StructField("prdtid", StringType, true),
      StructField("sectype", StringType, true),
      StructField("secsubtype", StringType, true),
      StructField("symbol", StringType, true)
    ))
    val prdDF=hiveContext.createDataFrame(newprd, structType2)
    var midStockDF = insertToData.join(prdDF,insertToData("securityid")===prdDF("prdtid"))
    //midStockDF=midStockDF.filter(midStockDF("securityid").equalTo("399007")).filter(midStockDF("origtime").equalTo("20180330093057000"));

    val localData=midStockDF.map(convertIndexType)
    println("需要补录的数据记录数："+localData.count())
    sc.hadoopConfiguration.set("hbase.zookeeper.quorum","arch-bd-zookeeper2,arch-bd-zookeeper4,arch-bd-zookeeper1,arch-bd-zookeeper3,arch-bd-zookeeper5")
    sc.hadoopConfiguration.set("hbase.zookeeper.property.clientPort", "2181")
    sc.hadoopConfiguration.set(TableOutputFormat.OUTPUT_TABLE, "MDC:MDIndexRecord")
    //指定输出格式和输出表名
    //val jobConf = new JobConf(hbaseconf, this.getClass)
    val jobConf = new Job(sc.hadoopConfiguration)
    jobConf.setOutputKeyClass(classOf[ImmutableBytesWritable])
    jobConf.setOutputValueClass(classOf[Result])
    jobConf.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])
    localData.saveAsNewAPIHadoopDataset(jobConf.getConfiguration)
  }

  def checkIndex(sc:SparkContext, hiveContext: HiveContext, year:String, month:String, day:String): String ={
    val secIDs:DataFrame=hiveContext.sql("select distinct(t.securityid),t.year,t.month,t.day from table_app_t_md_index t where  t.year='"+year+"' and t.month='"+month+"' and t.day='"+day+"'")
    val secc=secIDs.count();
    println("secID数量:"+secc)
    if(secc<=0){
      return null;
    }

    var oneRdd=secIDs.rdd.flatMap {
      case Row(securityid: String, year: String, month: String, day: String) => {
        val HTSCSecurityID = securityid + ".SZ"
        val start1 = year + month + day + "085959999"
        val end1 = year + month + day + "113000000"
        val startKey = MD5Hash.getMD5AsHex(HTSCSecurityID.getBytes).substring(0, 6) + HTSCSecurityID + (java.lang.Long.MAX_VALUE - sdf.parse(end1).getTime)
        val endKey = MD5Hash.getMD5AsHex(HTSCSecurityID.getBytes).substring(0, 6) + HTSCSecurityID + (java.lang.Long.MAX_VALUE - sdf.parse(start1).getTime)
        val conf = HBaseConfiguration.create()
        //设置zooKeeper集群地址，也可以通过将hbase-site.xml导入classpath，但是建议在程序里这样设置
        conf.set("hbase.zookeeper.quorum", "arch-bd-zookeeper2,arch-bd-zookeeper4,arch-bd-zookeeper1,arch-bd-zookeeper3,arch-bd-zookeeper5")
        // conf.set("hbase.htable.threads.max", "1000");
        conf.set("hbase.zookeeper.property.clientPort", "2181")
        //conf.setInt("hbase.rpc.timeout", 500000)

        //conf.setInt("zookeeper.session.timeout", 500000)
        import org.apache.hadoop.hbase.client.ConnectionFactory
        val conn = ConnectionFactory.createConnection(conf)
        val table: org.apache.hadoop.hbase.client.Table = conn.getTable(TableName.valueOf("MDC:MDIndexRecord"))
        val scan = new Scan();
       // scan.setMaxVersions
       // scan.setMaxResultSize(19000)
        import java.util

        import org.apache.hadoop.hbase.util.Bytes
        scan.setStartRow(Bytes.toBytes(startKey))
        scan.setStopRow(Bytes.toBytes(endKey))
       // scan.set
        val rs: org.apache.hadoop.hbase.client.ResultScanner= table.getScanner(scan)

        var aad=0;
        val it: util.Iterator[Result] = rs.iterator();
        while (it.hasNext) {
          val next: Result = it.next()
          if(null!=next){
            aad=aad+1
          }
        }

        //１３－５点的扫描开始
        val start2 = year + month + day + "125959999"
        val end2 = year + month + day + "150000000"
        val startKey2 = MD5Hash.getMD5AsHex(HTSCSecurityID.getBytes).substring(0, 6) + HTSCSecurityID + (java.lang.Long.MAX_VALUE - sdf.parse(end2).getTime)
        val endKey2 = MD5Hash.getMD5AsHex(HTSCSecurityID.getBytes).substring(0, 6) + HTSCSecurityID + (java.lang.Long.MAX_VALUE - sdf.parse(start2).getTime)

        val scan2 = new Scan();
        scan2.setStartRow(Bytes.toBytes(startKey2))
        scan2.setStopRow(Bytes.toBytes(endKey2))
        val rs3: org.apache.hadoop.hbase.client.ResultScanner= table.getScanner(scan2)

        var aad2:Int=0;
        val it3: util.Iterator[Result] = rs3.iterator()
        while (it3.hasNext) {
          val next: Result = it3.next()
          //if(null!=next){
            aad2=aad2+1
          //}
        }
        val size2:Int=(aad+aad2)
        var arr2=new Array[(String,String)](size2)
        if(size2<=0){
          arr2=new Array[(String,String)](1)
          arr2(0)=("000000", "000000")
        }else {
          for(ii <-0 until size2 ){
            arr2(ii)=("000000", "000000")
          }
          val scan3 = new Scan();
          scan3.setStartRow(Bytes.toBytes(startKey))
          scan3.setStopRow(Bytes.toBytes(endKey))
          val rs2: org.apache.hadoop.hbase.client.ResultScanner = table.getScanner(scan3)
          val it2: util.Iterator[Result] = rs2.iterator();
          var index = 0;
          while (it2.hasNext) {
            val next2: Result = it2.next()
            if (null != next2) {

              val mdTime = new String(next2.getValue(Bytes.toBytes("Detail"), Bytes.toBytes("MDTime")))
              // val applseqnum = new String(next2.getValue(Bytes.toBytes("Detail"), Bytes.toBytes("ApplSeqNum")))
              arr2(index) = (securityid, year + month + day + mdTime)

            } else {
              arr2(index) = ("000000", "000000")
            }
            index = index + 1
          }

          val scan4 = new Scan();
          scan4.setStartRow(Bytes.toBytes(startKey2))
          scan4.setStopRow(Bytes.toBytes(endKey2))
          val rs4: org.apache.hadoop.hbase.client.ResultScanner = table.getScanner(scan4)

          val it4: util.Iterator[Result] = rs4.iterator();

          while (it4.hasNext) {
            val next2: Result = it4.next()
            if (null != next2) {

              val mdTime = new String(next2.getValue(Bytes.toBytes("Detail"), Bytes.toBytes("MDTime")))
              // val applseqnum = new String(next2.getValue(Bytes.toBytes("Detail"), Bytes.toBytes("ApplSeqNum")))
              arr2(index) = (securityid, year + month + day + mdTime)

            } else {
              arr2(index) = ("000000", "000000")
            }
            index = index + 1
          }
          try {
            conn.close();
          } catch {
            case e: Exception => e.printStackTrace()
          }
        }
        arr2
      }
    }
    val oner=oneRdd.filter(row=>{
      row._1!="000000"
    }).map(row=>Row(row._1,row._2))
    val onerc=oner.count() //20605931

    val amstart=year+month+day+"090000000"
    val amend=year+month+day+"113000000"
    val pmstart=year+month+day+"130000000"
    val pmend=year+month+day+"150000000"
    val secIDs2:DataFrame=hiveContext.sql("select t.securityid,cast(t.origtime as string) origtime from table_app_t_md_index t where  t.year='"+year+"' and t.month='"+month+"' and t.day='"+day+"' and (t.origtime between '"+amstart+"' and '"+amend+"' or t.origtime between '"+pmstart+"' and '"+pmend+"')")
    val secIc=secIDs2.count()

    //离线文件hbase都有：
    val inters=secIDs2.rdd.intersection(oner)//
    val intersc=inters.count() //

    //离线文件有，hbase没有的（需要补充进去）
    val sub=secIDs2.rdd.subtract(oner);
    val subc=sub.count()
   // sub.sortBy()

    //离线文件没有，hbase有的
    val onerr=oner.subtract(secIDs2.rdd)
    val onerrc=onerr.count()
    //onerr.take(10)

    //保存离线文件有，hbase没有的
    sub.coalesce(1,true).saveAsTextFile(dataOutDir);
    //sub.saveAsTextFile();
    //保存hbase有，离线文件没有的
    if(onerrc>0) {
      onerr.coalesce(1, true).saveAsTextFile(dataOutDir2)
    }
    val statisticResult= "Hbase中数据量:"+onerc+ " 深交所离线文件数据量："+secIc+" 二者交叉交集数据量："+intersc+" 离线文件有，hbase没有的数据量："+subc +" 离线文件没有，hbase有的数据量："+onerrc
    println(statisticResult)

    if(insertRec==true){
      insertIndexData(sub,hiveContext,sc,year+month+day);
    }

    //index在每日对比文件最后顺序，仅在本文件添加是否成功标识
    if(intersc>0){
      HBaseUtil.writeStatus("OK");
    }else{
      HBaseUtil.writeStatus("CHECK");
    }

    return statisticResult
  }

  def convertIndexType(row: Row)= {
    val format=new SimpleDateFormat("yyyyMMddHHmmssSSS")
    val secid=row.getAs[String]("securityid")
    val HTSCSecurityID=secid+".SZ"
    val origtime=row.getAs[String]("origtime").substring(8);
    val MDDate= row.getAs[String]("origtime").substring(0,8);
    val origt=format.parse(row.getAs[String]("origtime"))
    val rowKey:String=MD5Hash.getMD5AsHex(HTSCSecurityID.getBytes).substring(0,6)+HTSCSecurityID+(java.lang.Long.MAX_VALUE-origt.getTime)
    val sectype=row.getAs[String]("sectype")
    val subType=row.getAs[String]("secsubtype")
    val symbol=row.getAs[String]("symbol")
    val mdstreamid=row.getAs[String]("mdstreamid")
    val p = new Put(Bytes.toBytes(rowKey))
    val trphaseCode=row.getAs[String]("tradingphasecode").trim

    var tpc:String=
      trphaseCode match{
        case "S0" =>"0";case "O0" =>"1";case "T0" =>"3";case "C0" =>"5";case "E0" =>"6";case "A0"=>"7";case "H0"=>"8";case "V0"=>"9";
        case "S1"=>"8";case "O1"=>"8";case "B1"=>"8";case "T1"=>"8";case "C1"=>"8";case "E1"=>"8";
        case "A1"=>"8";case "H1"=>"8";case  "V1"=>"8";
        case "S"=>"0";case "O"=>"1";case "T"=>"3";case "C"=>"5";
        case "E"=>"6";case "A"=>"7";case "H"=>"8";case "V"=>"9";
      };
    val hm=row.getAs[String]("origtime").substring(8,12)
    if((hm>="0925" && hm<="0930")){
      tpc="2"
    }
    if((hm>="1130" && hm<="1300")){
      tpc="4"
    }
    val preclosepx=row.getAs[Double]("preclosepx")
    val totalVolumnTrade=row.getAs[Long]("totalvolumetrade")
    val totalValueTrade=row.getAs[Double]("totalvaluetrade")
    val lastpx=row.getAs[Double]("lastpx")
    val openpx=row.getAs[Double]("openpx")
    val highpx=row.getAs[Double]("highpx")
    val lowpx=java.lang.Double.parseDouble(row.getAs[String]("lowpx"))

    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("MDDate"), Bytes.toBytes(MDDate))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("MDTime"), Bytes.toBytes(origtime))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("ReceiveDateTime"), Bytes.toBytes(row.getAs[String]("origtime")))
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
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("HTSCSecurityID"), Bytes.toBytes(HTSCSecurityID))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("MDLevel"), Bytes.toBytes("1"))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("MDChannel"), Bytes.toBytes("4"))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("MDRecordType"), Bytes.toBytes("1"))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("TradingPhaseCode"), Bytes.toBytes(tpc))
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

}
