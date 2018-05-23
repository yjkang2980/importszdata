package cn.com.htsc.hqcenter.dataretrive

import java.text.SimpleDateFormat

import org.apache.commons.lang.StringUtils
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
  *用spark对行情数据文件进行校验和缺失导入，缺失导入类四个参数分别表示：年、月、日、是否在校验完成后将缺失数据入库,T表示入库
  * @author 010571 
  * @version $Id:
  * public static final int IndexType_VALUE = 1; 指数，
  * public static final int StockType_VALUE = 2; 股票......
  * public static final int FundType_VALUE = 3; 基金.......
  * public static final int BondType_VALUE = 4; 债券.....
  * MDC:MDFundRecord 基金
  * MDC:MDBondRecord 债券
  *
  */
object SZFundDataFileAndHBaseCom {
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

  var resultDir="/user/u010571/dataretrieve/"+year+month+day+"/fund/statistic"
  var dataOutDir="/user/u010571/dataretrieve/"+year+month+day+"/fund/szHasHbaseNull"
  var dataOutDir2="/user/u010571/dataretrieve/"+year+month+day+"/fund/HbaseHasszNull"

  val snapt = "table_app_t_md_snap"
  val snaplevelt = "table_app_t_md_snap_level"
  val ordert = "table_app_t_md_order"
  val tradet = "table_app_t_md_trade"
  val  indext="table_app_t_md_index"

  var insertRec=false

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("hqszfunddatacompare")
    val sc = new SparkContext(conf)
    val hiveContext = new HiveContext(sc)
    hiveContext.sql("use mdc")
   //val year="2018"
    //val mon="03"
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
    resultDir="/user/u010571/dataretrieve/"+year+month+day+"/fund/statistic"
    dataOutDir="/user/u010571/dataretrieve/"+year+month+day+"/fund/szHasHbaseNull"
    dataOutDir2="/user/u010571/dataretrieve/"+year+month+day+"/fund/HbaseHasszNull"

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

    val result=checkSnap(sc,hiveContext,year,month,day);

    if(null!=result){
      sc.makeRDD(Array(result)).coalesce(1, true).saveAsTextFile(resultDir);
      //完成后，上传到ftp
      /*if(hdfs.exists(new Path(resultDIr+"/part-00000"))){
        val in = hdfs.open(new Path(resultDIr+"/part-00000"))
        UPloadToFtp.uploadToFtp(in, "/hjp/"+year+month+day+"/fund/statistic", "part-00000.txt")
      }
      if(hdfs.exists(new Path(dataOutDir+"/part-00000"))){
        val in = hdfs.open(new Path(dataOutDir+"/part-00000"))
        UPloadToFtp.uploadToFtp(in, "/hjp/"+year+month+day+"/fund/szHasHbaseNull", "part-00000.txt")
      }*/
    }
  }

  def insertSnapRec(sub1: RDD[Row], hiveContext: HiveContext, sc: SparkContext, ymd: String) = {
    val structType = StructType(Array(
      StructField("securityid", StringType, true),
      StructField("origtime", StringType, true)
    ))
    val intersect = hiveContext.createDataFrame(sub1, structType)
    intersect.registerTempTable("mdc_snap_intersectdata")

    val prdinfoRdd = sc.textFile("hdfs://nameservice1/user/u010571/data/prdInfo.txt")

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
    prdDF.registerTempTable("mdc_prdtInfos")

    val snapsql="select  * from table_app_t_md_snap t, mdc_prdtInfos p,table_app_t_md_snap_level q,mdc_snap_intersectdata r where t.year='"+ymd.substring(0,4)+"' and t.month='"+ymd.substring(4,6)+"' and t.day='"+ymd.substring(6,8)+"' and q.year='"+ymd.substring(0,4)+"' and q.month='"+ymd.substring(4,6)+"' and q.day='"+ymd.substring(6,8)+"' and t.securityid=q.securityid and t.origtime=q.origtime and t.securityid=r.securityid and t.origtime=r.origtime and p.sectype='3' and p.prdtid=t.securityid "
    var insertToData = hiveContext.sql(snapsql)
    insertToData.persist()
    // prdDF.cache();
    //val insertToData = intersect.join(snapData, Seq("securityid", "origtime"), "inner")

    // var midStockDF = insertToData.join(prdDF,insertToData("securityid")===prdDF("prdtid"))
    //002609.SZ,20180330093100000
    // insertToData=insertToData.filter(insertToData("prdtid").equalTo("002609")).filter(insertToData("t.origtime").equalTo("20180330093100000"));

    val localData=insertToData.map(convertStockFundType)
    println("需要补录的数据记录数："+localData.count())
    sc.hadoopConfiguration.set("hbase.zookeeper.quorum","arch-bd-zookeeper2,arch-bd-zookeeper4,arch-bd-zookeeper1,arch-bd-zookeeper3,arch-bd-zookeeper5")
    sc.hadoopConfiguration.set("hbase.zookeeper.property.clientPort", "2181")
    sc.hadoopConfiguration.set(TableOutputFormat.OUTPUT_TABLE, "MDC:MDFundRecord")
    //指定输出格式和输出表名
    //val jobConf = new JobConf(hbaseconf, this.getClass)
    val jobConf = new Job(sc.hadoopConfiguration)
    jobConf.setOutputKeyClass(classOf[ImmutableBytesWritable])
    jobConf.setOutputValueClass(classOf[Result])
    jobConf.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])
    localData.saveAsNewAPIHadoopDataset(jobConf.getConfiguration)
  }

  def checkSnap(sc:SparkContext, hiveContext: HiveContext, year:String, mon:String, day:String): String ={
   //  val secIDs:DataFrame=hiveContext.sql("select p.securityid,'"+year+"' as year,'"+mon+"' as month,'"+day+"' as day from mdc_center_prdt_info p where  p.sectype='2' and instr(p.securityid,'.SZ')>0")
    // secIDs.persist()

    val amstart=year+mon+day+"090000000"
    val amend=year+mon+day+"113000000"
    val pmstart=year+mon+day+"130000000"
    val pmend=year+mon+day+"150000000"
    var sql="select  distinct(concat(t.securityid,'.SZ')) as securityid,'"+year+"' as year,'"+mon+"' as month,'"+day+"' as day from table_app_t_md_snap t, mdc_center_prdt_info p where t.year='"+year+"' and t.month='"+mon+"' and t.day='"+day+"' and p.sectype='3' and instr(p.securityid,'SZ')>0 and p.securityid=concat(t.securityid,'.SZ') and (t.origtime between '"+amstart+"' and '"+amend+"' or t.origtime between '"+pmstart+"' and '"+pmend+"')"
    println(" hive 中的sql查询secid集合:"+sql )
    val secIDs:DataFrame=hiveContext.sql(sql)
    secIDs.persist()
    val secc=secIDs.count();
    println("secID数量:"+secc)
    if(secc<=0){
      return null;
    }
    var oneRdd=secIDs.rdd.flatMap {
      case Row(securityid: String, year: String, month: String, day: String) => {
        val HTSCSecurityID = securityid
        val start1 = year + month + day + "085959999"
        val end1 = year + month + day + "113000000"
        val startKey = MD5Hash.getMD5AsHex(HTSCSecurityID.getBytes).substring(0, 6) + HTSCSecurityID + (java.lang.Long.MAX_VALUE - sdf.parse(end1).getTime)
        val endKey = MD5Hash.getMD5AsHex(HTSCSecurityID.getBytes).substring(0, 6) + HTSCSecurityID + (java.lang.Long.MAX_VALUE - sdf.parse(start1).getTime)
        val conf = HBaseConfiguration.create()
        //设置zooKeeper集群地址，也可以通过将hbase-site.xml导入classpath，但是建议在程序里这样设置
        conf.set("hbase.zookeeper.quorum", "arch-bd-zookeeper2,arch-bd-zookeeper4,arch-bd-zookeeper1,arch-bd-zookeeper3,arch-bd-zookeeper5")
        // conf.set("hbase.htable.threads.max", "1000");
        conf.set("hbase.zookeeper.property.clientPort", "2181")
        //conf.setInt("hbase.rpc.timeout", 60000)
        //conf.setInt("zookeeper.session.timeout", 500000)
        import org.apache.hadoop.hbase.client.ConnectionFactory
        val conn = ConnectionFactory.createConnection(conf)
        val table: org.apache.hadoop.hbase.client.Table = conn.getTable(TableName.valueOf("MDC:MDFundRecord"))
        val scan = new Scan();
       // scan.setMaxVersions
       // scan.setMaxResultSize(19000)
        import java.util

        import org.apache.hadoop.hbase.util.Bytes
        scan.setStartRow(Bytes.toBytes(startKey))
        scan.setStopRow(Bytes.toBytes(endKey))
        val rs: org.apache.hadoop.hbase.client.ResultScanner= table.getScanner(scan)

        var aad=0;
        val it: util.Iterator[Result] = rs.iterator();
        while (it.hasNext) {
          val next: Result = it.next()
          //if(null!=next) {
          ///  val mdTime = new String(next.getValue(Bytes.toBytes("Detail"), Bytes.toBytes("MDTime")))
           // if(null!=mdTime && null!=securityid){
              aad = aad + 1
           // }

         // }
        }

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
          //  val mdTime = new String(next.getValue(Bytes.toBytes("Detail"), Bytes.toBytes("MDTime")))
          //  if(null!=mdTime && null!=securityid) {
              aad2 = aad2 + 1
          //  }
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
              //if (null != mdTime && null != securityid) {
                arr2(index) = (securityid, year + month + day + mdTime)
            }else{
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
              //if (null != mdTime && null != securityid) {
                arr2(index) = (securityid, year + month + day + mdTime)
              //}

            } else{
             arr2(index) = ("000000", "000000")
            }
            index = index + 1
          }

          try {
            conn.close();
          } catch {
            case e: Exception => e.printStackTrace()
          }
          println("secID:" + securityid + " hbasesize:" + size2 + " length:" + arr2.length)
        }
        //val eee=sc.makeRDD(arr2)
       // eee
        arr2
      }
    }
   // oneRdd.coalesce(1, true).saveAsTextFile("/user/u010571/1009/savesnaprdd")
    //oneRdd.persist()
    //oneRdd=oneRdd.filter{rdd=>{Some(rdd)==true}}
    oneRdd.persist()
   val onerc1=oneRdd.count() //20171009---6263452
    //println("hbase中查询的"+year+"-"+month+"--"+day+"　ｈｂａｓｅ中数据量："+onerc1)
    //if(onerc1 <=0){
    // return null;
    //}

    val oner=oneRdd
      .map{case(row1,row2)=>(row1,row2)}
      .filter{case (row1,row2)=>{
      null!=row1 && row1!="000000" && null!=row2 && row2!="000000"
    }}.map(row=>Row(row._1,row._2))

   // val oner=oneRdd.filter{case(row1,row2)=>{
     // println("row值："+row1)
    //  null!=row1 && row1!="000000" && null!=row2 && row2!="000000"
    //}}.map(row=>Row(row._1,row._2))
    oner.persist()
    val onerc=oner.count() //20605931    3547 1774
    //if(onerc <=0){
    //  return null;
    //}
    println("hbase中查询的"+year+"-"+month+"--"+day+"　ｈｂａｓｅ中数据量："+onerc1+" 过滤后数量："+onerc)
    sql="select  concat(t.securityid,'.SZ') as securityid,origtime from table_app_t_md_snap t, mdc_center_prdt_info p where t.year='"+year+"' and t.month='"+mon+"' and t.day='"+day+"' and p.sectype='3' and instr(p.securityid,'SZ')>0 and p.securityid=concat(t.securityid,'.SZ') and (t.origtime between '"+amstart+"' and '"+amend+"' or t.origtime between '"+pmstart+"' and '"+pmend+"')"

    val secIDs2:DataFrame=hiveContext.sql(sql)
    secIDs2.persist()
    val secIc=secIDs2.count()

    //离线文件hbase都有：
    val inters=secIDs2.rdd.intersection(oner)//
    val intersc=inters.count() //

    //离线文件有，hbase没有的（需要补充进去）
    var sub=secIDs2.rdd.subtract(oner);
    val sub1=sub.map(row=>{
      Row(row.getString(0).substring(0,row.getString(0).length-3),row.getString(1))
    })
    sub1.persist()
    val subc=sub1.count()
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

    if(insertRec==true) {
      insertSnapRec(sub1,hiveContext,sc,year+mon+day)

    }

    return statisticResult
  }

  def convertStockFundType(row: Row): (ImmutableBytesWritable, Put) = {
    // val rb = ResourceBundle.getBundle("prdInfo".trim)
    //val keys=rb.getKeys
    val format = new SimpleDateFormat("yyyyMMddHHmmssSSS")
    var secid = row.getAs[String]("securityid")
    val mdstreamid = row.getAs[String]("mdstreamid")
    val origtime = row.getAs[String]("origtime")
    val MDDate = row.getAs[String]("tradedate");
    val origt = format.parse(origtime)
    val preclosepx = row.getAs[Double]("preclosepx")
    val pxchange1 = row.getAs[Double]("pxchange1")
    val pxchange2 = row.getAs[Double]("pxchange2")
    val openpx = row.getAs[Double]("openpx")
    val highpx = row.getAs[Double]("highpx")
    val lowpx = row.getAs[Double]("lowpx")
    val lastpx = row.getAs[Double]("lastpx")
    val numtrades = row.getAs[Long]("numtrades")
    val totalVolumnTrade = row.getAs[Long]("totalvolumetrade")
    val totalValueTrade = row.getAs[Double]("totalvaluetrade")
    val uplimitpx =row.getAs[Double]("uplimitpx")
    val downlimitpx = row.getAs[Double]("downlimitpx")
    var totalofferqty =row.getAs[String]("totalofferqty")
    var totalbidQty = row.getAs[String]("totalbidqty")
    val PreIOPV=row.getAs[Double]("prenav")
    val IOPV=row.getAs[String]("realtimenav")

    if (totalofferqty.contains(".")) {
      totalofferqty = totalofferqty.split("\\.", 2)(0)
    }
    if (totalbidQty.contains(".")) {
      totalbidQty = totalbidQty.split("\\.", 2)(0)
    }

    val weightebidpx = row.getAs[Double]("weightedavgbidpx")
    val weiavgofferpx =row.getAs[Double]("weightedavgofferpx")
    val peratio1 = row.getAs[Double]("peratio1")
    val peratio2 = row.getAs[Double]("peratio2")
    val trphaseCode = row.getAs[String]("tradingphasecode").trim
    val warrantrate = row.getAs[Double]("warrantpremiumrate")
    val bidpx1 = row.getAs[Double]("bidpx1")
    val bidsize1 = row.getAs[Long]("bidsize1")
    val numorderb1 = row.getAs[Long]("numorders_b1")
    val noorderb1 = row.getAs[Long]("noorders_b1")
    val orderqtyb1 =row.getAs[String]("orderqty_b1")

    val offerpx1 = row.getAs[Double]("offerpx1")
    val offersize1 = row.getAs[Long]("offersize1")
    val numorders1 = row.getAs[Long]("numorders_s1")
    val noorders1 = row.getAs[Long]("noorders_s1")
    val orderqtys1 = row.getAs[String]("orderqty_s1")


    val offerpx2 = row.getAs[Double]("offerpx2")
    val offersize2 =  row.getAs[Long]("offersize2")
    val bidpx2 =  row.getAs[Double]("bidpx2")
    val bidsize2 =  row.getAs[Long]("bidsize2")
    val bidpx3 =  row.getAs[Double]("bidpx3")
    val bidsize3 =  row.getAs[Long]("bidsize3")
    val offerpx3 =  row.getAs[Double]("offerpx3")
    val offersize3 =  row.getAs[Long]("offersize3")
    val offerpx4 =  row.getAs[Double]("offerpx4")
    val offersize4 =  row.getAs[Long]("offersize4")
    val bidpx4 =  row.getAs[Double]("bidpx4")
    val bidsize4 =  row.getAs[Long]("bidsize4")
    val bidpx5 =  row.getAs[Double]("bidpx5")
    val bidsize5 =  row.getAs[Long]("bidsize5")
    val offerpx5 =  row.getAs[Double]("offerpx5")
    val offersize5 =  row.getAs[Long]("offersize5")
    val offerpx6 =  row.getAs[Double]("offerpx6")
    val offersize6 =  row.getAs[Long]("offersize6")
    val bidpx6 =  row.getAs[Double]("bidpx6")
    val bidsize6 =  row.getAs[Long]("bidsize6")
    val bidpx7 =  row.getAs[Double]("bidpx7")
    val bidsize7 =  row.getAs[Long]("bidsize7")
    val offerpx7 =  row.getAs[Double]("offerpx7")
    val offersize7 =  row.getAs[Long]("offersize7")
    val offerpx8 =  row.getAs[Double]("offerpx8")
    val offersize8 =  row.getAs[Long]("offersize8")
    val bidpx8 =  row.getAs[Double]("bidpx8")
    val bidsize8 =  row.getAs[Long]("bidsize8")
    val bidpx9 =  row.getAs[Double]("bidpx9")
    val bidsize9 =  row.getAs[Long]("bidsize9")
    val offerpx9 =  row.getAs[Double]("offerpx9")
    val offersize9 =  row.getAs[Long]("offersize9")
    val offerpx10 =  row.getAs[Double]("offerpx10")
    val offersize10 =  row.getAs[Long]("offersize10")
    val bidpx10 =  row.getAs[Double]("bidpx10")
    val bidsize10 =  row.getAs[Long]("bidsize10")

    println("-------------------77777------------------")
    var tpc: String =
      trphaseCode match {
        case "S0" => "0";
        case "O0" => "1";
        case "T0" => "3";
        case "C0" => "5";
        case "E0" => "6";
        case "A0" => "7";
        case "H0" => "8";
        case "V0" => "9";
        case "S1" => "8";
        case "O1" => "8";
        case "B1" => "8";
        case "T1" => "8";
        case "C1" => "8";
        case "E1" => "8";
        case "A1" => "8";
        case "H1" => "8";
        case "V1" => "8";
        case "S" => "0";
        case "O" => "1";
        case "T" => "3";
        case "C" => "5";
        case "E" => "6";
        case "A" => "7";
        case "H" => "8";
        case "V" => "9";
        case "B0" => "4";
      };
    val hm = row.getString(2).substring(8, 12)
    if ((hm >= "0925" && hm <= "0930")) {
      tpc = "2"
    }
    if ((hm >= "1130" && hm <= "1300")) {
      tpc = "4"
    }

    val HTSCSecurityID = secid+".SZ"
    //secid=secid.substring(0,secid.length-3)
    val rowKey: String = MD5Hash.getMD5AsHex(HTSCSecurityID.getBytes()).substring(0, 6) + HTSCSecurityID + (java.lang.Long.MAX_VALUE - origt.getTime)

    //val props = rb.getString(HTSCSecurityID)
    //val str = props.trim.split(",")
    val sectype = row.getAs[String]("sectype")
    val subType =row.getAs[String]("secsubtype")
    val symbol = row.getAs[String]("symbol")
    val p = new Put(Bytes.toBytes(rowKey))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("MDDate"), Bytes.toBytes(MDDate))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("MDTime"), Bytes.toBytes(origtime.substring(8)))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("ReceiveDateTime"), Bytes.toBytes(origtime))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("MDStreamID"), Bytes.toBytes(mdstreamid))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("SecurityType"), Bytes.toBytes(sectype))
    if ("-" != subType) {
      p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("SecuritySubType"), Bytes.toBytes(subType))
    }

    //基金深交所数据有 PreNAV 和 RealTimeNAV t-1净值和实时参考净值 但是行情中心没有这两个属性
    if(null!=IOPV && !IOPV.equals("NULL")){
      p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("IOPV"), Bytes.toBytes(IOPV))
    }
    if(null!=PreIOPV && !PreIOPV.equals("NULL")) {
      p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("PreIOPV"), Bytes.toBytes(PreIOPV.toString))
    }
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("SecurityID"), Bytes.toBytes(secid))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("SecurityIDSource"), Bytes.toBytes("102"))
    if ("-" != symbol) {
      p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("Symbol"), Bytes.toBytes(symbol))
    }
    //HTSCSecurityID
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("HTSCSecurityID"), Bytes.toBytes(HTSCSecurityID))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("MDLevel"), Bytes.toBytes("1"))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("MDChannel"), Bytes.toBytes("4"))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("MDRecordType"), Bytes.toBytes("1"))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("TradingPhaseCode"), Bytes.toBytes(tpc))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("PreClosePx"), Bytes.toBytes(preclosepx.toString))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("NumTrades"), Bytes.toBytes(numtrades.toString))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("TotalVolumeTrade"), Bytes.toBytes(totalVolumnTrade.toString))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("TotalValueTrade"), Bytes.toBytes(totalValueTrade.toString))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("LastPx"), Bytes.toBytes(lastpx.toString))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("OpenPx"), Bytes.toBytes(openpx.toString))
    // p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("ClosePx"), Bytes.toBytes())
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("HighPx"), Bytes.toBytes(highpx.toString))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("LowPx"), Bytes.toBytes(lowpx.toString))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("DiffPx1"), Bytes.toBytes(pxchange1.toString))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("DiffPx2"), Bytes.toBytes(pxchange2.toString))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("MaxPx"), Bytes.toBytes(uplimitpx.toString))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("MinPx"), Bytes.toBytes(downlimitpx.toString))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("TotalBidQty"), Bytes.toBytes(totalbidQty.toString))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("TotalOfferQty"), Bytes.toBytes(totalofferqty.toString))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("WeightedAvgBidPx"), Bytes.toBytes(weightebidpx.toString))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("WeightedAvgOfferPx"), Bytes.toBytes(weiavgofferpx.toString))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("SLYOne"), Bytes.toBytes(peratio1.toString))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("SLYTwo"), Bytes.toBytes(peratio2.toString))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("Buy1Price"), Bytes.toBytes(bidpx1.toString))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("Buy1OrderQty"), Bytes.toBytes(bidsize1.toString))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("Buy1NumOrders"), Bytes.toBytes(numorderb1.toString))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("Buy1NoOrders"), Bytes.toBytes(noorderb1.toString))

    if (StringUtils.isNotBlank(orderqtyb1.trim)) {
      val aa: List[String] = orderqtyb1.trim.split("\\|").toList;
      val ld: java.util.List[Double] = new java.util.ArrayList[Double]
      for (a <- aa) {
        ld.add(java.lang.Double.parseDouble(a))
      }
      p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("Buy1OrderDetail"), Bytes.toBytes(ld.toString))

    }

    //p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("Buy1OrderDetail"), Bytes.toBytes(orderqtyb1))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("Sell1Price"), Bytes.toBytes(offerpx1.toString))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("Sell1OrderQty"), Bytes.toBytes(offersize1.toString))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("Sell1NumOrders"), Bytes.toBytes(numorders1.toString))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("Sell1NoOrders"), Bytes.toBytes(noorders1.toString))
    if (StringUtils.isNotBlank(orderqtys1.trim)) {
      val aa: List[String] = orderqtys1.trim.split("\\|").toList;
      //val bb:List[String]=orderqtyb1.trim.split("\\|").toList;
      val ld: java.util.List[Double] = new java.util.ArrayList[Double]
      // val ld2: java.util.List[Double] = new java.util.ArrayList[Double]
      for (a <- aa) {
        ld.add(java.lang.Double.parseDouble(a))
      }
      p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("Sell1OrderDetail"), Bytes.toBytes(ld.toString)) //----
    }
   // p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("Sell1OrderDetail"), Bytes.toBytes(orderqtys1))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("Buy2Price"), Bytes.toBytes(bidpx2.toString))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("Buy2OrderQty"), Bytes.toBytes(bidsize2.toString))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("Sell2Price"), Bytes.toBytes(offerpx2.toString))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("Sell2OrderQty"), Bytes.toBytes(offersize2.toString))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("Buy3Price"), Bytes.toBytes(bidpx3.toString))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("Buy3OrderQty"), Bytes.toBytes(bidsize3.toString))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("Sell3Price"), Bytes.toBytes(offerpx3.toString))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("Sell3OrderQty"), Bytes.toBytes(offersize3.toString))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("Buy4Price"), Bytes.toBytes(bidpx4.toString))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("Buy4OrderQty"), Bytes.toBytes(bidsize4.toString))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("Sell4Price"), Bytes.toBytes(offerpx4.toString))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("Sell4OrderQty"), Bytes.toBytes(offersize4.toString))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("Buy5Price"), Bytes.toBytes(bidpx5.toString))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("Buy5OrderQty"), Bytes.toBytes(bidsize5.toString))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("Sell5Price"), Bytes.toBytes(offerpx5.toString))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("Sell5OrderQty"), Bytes.toBytes(offersize5.toString))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("Buy6Price"), Bytes.toBytes(bidpx6.toString))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("Buy6OrderQty"), Bytes.toBytes(bidsize6.toString))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("Sell6Price"), Bytes.toBytes(offerpx6.toString))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("Sell6OrderQty"), Bytes.toBytes(offersize6.toString))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("Buy7Price"), Bytes.toBytes(bidpx7.toString))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("Buy7OrderQty"), Bytes.toBytes(bidsize7.toString))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("Sell7Price"), Bytes.toBytes(offerpx7.toString))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("Sell7OrderQty"), Bytes.toBytes(offersize7.toString))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("Buy8Price"), Bytes.toBytes(bidpx8.toString))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("Buy8OrderQty"), Bytes.toBytes(bidsize8.toString))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("Sell8Price"), Bytes.toBytes(offerpx8.toString))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("Sell8OrderQty"), Bytes.toBytes(offersize8.toString))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("Buy9Price"), Bytes.toBytes(bidpx9.toString))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("Buy9OrderQty"), Bytes.toBytes(bidsize9.toString))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("Sell9Price"), Bytes.toBytes(offerpx9.toString))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("Sell9OrderQty"), Bytes.toBytes(offersize9.toString))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("Buy10Price"), Bytes.toBytes(bidpx10.toString))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("Buy10OrderQty"), Bytes.toBytes(bidsize10.toString))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("Sell10Price"), Bytes.toBytes(offerpx10.toString))
    p.addColumn(Bytes.toBytes("Detail"), Bytes.toBytes("Sell10OrderQty"), Bytes.toBytes(offersize10.toString))
    //  println("返回stocktype："+typek+"--"+sectype)
    (new ImmutableBytesWritable, p)
  }

}
