package cn.com.htsc.hqcenter.outproperties

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

/**
  *
  * @author
  * @version $Id:
  *
  */
object Test {


  def main(args: Array[String]): Unit = {
       println("902123" > "901")

val row="20160831123024000"

    val trphaseCode="T"
    var tpc=
      trphaseCode match{
        case "S0" =>"0";case "O0" =>"1";case "T0" =>"3";case "C0" =>"5";case "E0" =>"6";case "A0"=>"7";case "H0"=>"8";case "V0"=>"9";
        case "S1"=>"8";case "O1"=>"8";case "B1"=>"8";case "T1"=>"8";case "C1"=>"8";case "E1"=>"8";
        case "A1"=>"8";case "H1"=>"8";case  "V1"=>"8";
        case "S"=>"0";case "O"=>"1";case "T"=>"3";case "C"=>"5";case "E"=>"6";case "A"=>"7";case "H"=>"8";case "V"=>"9"

      };
    /*val hm=row.substring(8,12)
    if((hm>="0925" && hm<="0930")){
      tpc="2"
    }
    if((hm>="1130" && hm<="1300")){
      tpc="4"
    }*/
   println(tpc)



    println("asd.234123".contains("."))
    import org.apache.hadoop.hbase.util.MD5Hash
    import java.text.SimpleDateFormat
    val orgtime1: String = "20160831100500000"
    val sdf: SimpleDateFormat = new SimpleDateFormat("yyyyMMddHHmmssSSS")
    var dd: java.util.Date = sdf.parse(orgtime1)
    //val d: String = MDRowkeyUtils.getRowKey4Market("399009.SZ", dd)
    val rowKey: String = MD5Hash.getMD5AsHex("399007.SZ".getBytes).substring(0, 6) + "399007.SZ" + (java.lang.Long.MAX_VALUE - dd.getTime)

    println(rowKey)


    val MDDate= "20150106";
    val ddd=93020260;
    var origtime=new java.lang.Long(ddd).toString;
    if(origtime.length==8){
      origtime="0"+origtime
    }
    origtime=MDDate+origtime
    dd=sdf.parse(origtime)
    println("asdfasdf:" +origtime+" time"+ dd+"---"+java.lang.Long.MAX_VALUE)

    //order index
    val HTSCSecurityID ="000777.SZ"
    val orgtime2: String = "20180307133043670"
    val rowKey2:String=MD5Hash.getMD5AsHex(HTSCSecurityID.getBytes).substring(0,6)+HTSCSecurityID+
      (java.lang.Long.MAX_VALUE-sdf.parse(orgtime2).getTime)+( java.lang.Long.MAX_VALUE-6626594);
    val orgtime3: String = "20180307170000000"
    val rowKey1:String=MD5Hash.getMD5AsHex(HTSCSecurityID.getBytes).substring(0,6)+HTSCSecurityID+
      (java.lang.Long.MAX_VALUE-sdf.parse(orgtime3).getTime)+( java.lang.Long.MAX_VALUE-3198292);
    println(rowKey2)
    println(rowKey1)

    val df1:java.text.DecimalFormat=new java.text.DecimalFormat("####0.000")
    val totalM:Double=1.17d * 1000005L;
    val tradeMoney=java.lang.Double.parseDouble(df1.format(totalM))
    println(tradeMoney)
    val dtos:Double=1.2256;
    println("doubletoString:"+dtos.toString)

    val t1="4300|2100|2500|3000";val t2="300"
    val aa:List[String]=t1.split("\\|").toList;
    val bb:List[String]=t2.split("\\|").toList;
    val ld: java.util.List[Double] = new java.util.ArrayList[Double]
    val ld2: java.util.List[Double] = new java.util.ArrayList[Double]
   val tt="asdf "


    for(a<-aa){
      ld.add(java.lang.Double.parseDouble(a))
    }
    for(a<-bb){
      ld2.add(java.lang.Double.parseDouble(a))
    }

   System.out.println(ld.toString +"-"+ld2.toString+tt.trim)



//dataframe的测试用例
    val conf = new SparkConf().setAppName("hqdataimport")
    val sc = new SparkContext(conf)
    println("1-------------------------------------")
    val hiveContext = new HiveContext(sc)
    hiveContext.sql("use mdc")
    var rdd:RDD[(String,String,Int)] = sc.makeRDD(List(("k01","k02",3),("k01","k02",6),("k01","k02",7),("k01","k03",2),("k01","k04",26)))
    val rdd1= rdd.map(row=>{Row(row._1,row._2,row._3)})

    val sType = StructType(Array(
      StructField("c1", StringType, true),
      StructField("c2", StringType, true),
      StructField("c3", IntegerType, true)
    ))
    val prdDF1=hiveContext.createDataFrame(rdd1, sType)
    val prdDF2=prdDF1.orderBy(prdDF1("c3").desc).dropDuplicates(Seq("c1","c2"))
  }


}
