package DataPreprocess

import java.text.SimpleDateFormat

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

import scala.collection.mutable.ArrayBuffer
import GeneralFunctionSets.transTimeToTimestamp

object AFCDataExtract {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    val rdd = spark.read.option("header",true).csv(s"/data/AFCdata").rdd.map(fields => {
      val afcidN = fields(0).toString.toInt
      val time = fields(1).toString
      val station = fields(2).toString
      val tag = fields(3).toString.toInt //21: tap-in, 22: tap-out
      (afcidN,(time, station, tag))
    }).groupByKey().map(v =>(v._1,v._2.toArray.sortBy(_._1)))

    //将AFC轨迹中正常的OD对提取出来
    //(669404508,2019-06-01 09:21:28,世界之窗,21,2019-06-01 09:31:35,深大,22)
    val arr = new ArrayBuffer[String]()
    rdd.flatMap({ fields =>  //("卡号", Array[("交易时间", "线路站点","交易类型")])
      arr.clear()
      val tra: Array[(String, String, Int)] = fields._2 //("交易时间", "线路站点","交易类型")

      var i = 0
      while (i < tra.length - 1) {
        if (tra(i)._3==21  && tra(i + 1)._3==22) { //"地铁入站": 21  "地铁出站": 22
          val timeInterval=math.abs(transTimeToTimestamp(tra(i+1)._1)-transTimeToTimestamp(tra(i)._1))/3600.0
          if(timeInterval<=5)//时间间隔大于5小时
            arr += Array(fields._1,tra(i)._1,tra(i)._2,tra(i)._3,tra(i + 1)._1,tra(i + 1)._2,tra(i + 1)._3).mkString(",")

          i = i + 2
        } else {
          i = i + 1
        }
      } //end while

      arr
    }).saveAsTextFile("/data/AFCtrip")

  }//end main

}
