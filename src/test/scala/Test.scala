package util

import java.io.File

import org.apache.commons.lang.StringUtils

import scala.collection.mutable.ArrayBuffer

/**
  * Created by K0260006 on 2017/8/10.
  */
object Test {

  def main(args: Array[String]): Unit = {

   // for (d <- subDir(new File(this.getClass.getClassLoader.getResource("sql").getPath)))
   //   println(d)
    val d:String="    "
    println("adf:"+(d.trim==""))

    //  val dirs = dir.listFiles().filter(_.isDirectory())
  }

  def subDir(dir: File): Iterator[File] = {
    val dirs = dir.listFiles().filter(_.isDirectory())
    val files = dir.listFiles().filter(_.isFile())
    files.toIterator ++ dirs.toIterator.flatMap(subDir _)
  }

  def getWords(lines: Seq[String]): Seq[String] = lines.flatMap(line => line.split("\\W+"))


}
