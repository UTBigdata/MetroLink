package DataPreprocess

import java.text.SimpleDateFormat
import java.util.{Calendar, TimeZone}

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.math.abs
import GeneralFunctionSets.{hourOfDay_long, transTimeToTimestamp, transTimeToString}

object SplitAPData {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("SplitAPData")
      .getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    // 读取地铁站点名和编号映射关系 "1,机场东,22.647011,113.8226476,1268036000,268"
    val stationFile = sc.textFile("/data/stationInfo-UTF-8.txt")
    val stationNo2NameRDD = stationFile.map(line => {
      val stationNo = line.split(',')(0)
      val stationName = line.split(',')(1)
      (stationNo.toInt, stationName)
    })
    val stationNo2Name = sc.broadcast(stationNo2NameRDD.collect().toMap)

    //shortpath数据格式：
    //“1 2 2.6000”对应 “O ... D 最短时间” 时间的单位：min
    val readODTimeInterval = sc.textFile(s"/data/shortpath2.txt").map(line => {
      val p = line.split(' ')
      val sou = stationNo2Name.value(p(0).toInt)
      val des = stationNo2Name.value(p(1).toInt)
      val interval = math.ceil((p(2).toDouble*60)).toLong
      ((sou, des), interval)
    })
    val ODIntervalMap = sc.broadcast(readODTimeInterval.collect().toMap)

    // 读取所有有效路径的数据 "1 2 3 4 5 # 0 V 0.0000 12.6500"  “O ... D # 换乘次数 V 换乘时间 总时间”
    val validPathFile= sc.textFile("/data/allpath2.txt").map(line => {
      val fields = line.split(' ').dropRight(5) //只保留“1 2 3 4 5”
      val sou = stationNo2Name.value(fields(0).toInt)
      val des = stationNo2Name.value(fields(fields.length - 1).toInt)
      val path = fields.map(x => stationNo2Name.value(x.toInt))
      ((sou, des), path)
    }).groupByKey().mapValues(_.toArray) //RDD[((sou, des), Array[path])]
    val validPathMap: Broadcast[Map[(String, String), Array[Array[String]]]] = sc.broadcast(validPathFile.collect().toMap)

    val macFile = spark.read.option("header",true).csv("/data/WiFidata").rdd.map(fields=>{
      val apidN = fields(0).toString.toInt
      val time = transTimeToTimestamp(fields(1).toString)
      val station = fields(2).toString
      (apidN,(time, station))
    }).groupByKey().mapValues(_.toArray.sortBy(_._1))

    // 划分为出行片段
    val APSegments = macFile.map(line => { //(macId, Array[(time, station)])
      // 设置出行片段长度阈值
      val m = 1
      val MacId = line._1
      // 未划分序列
      val data = line._2 //Array[(time, station)]
      // 存储单个出行片段
      val segment = new ListBuffer[(Long, String)]

      // 存储所有出行片段
      val segments = new ListBuffer[List[(Long, String)]]
      for (s <- data) { //s: (time, station)
        if (segment.isEmpty) {
          segment.append(s)
        }
        else {
          // 只根据轨迹点之间的时间差判断是否分割
          // 根据OD时间长度设置额外容忍时间误差
          var extra = 0
          val odInterval = ODIntervalMap.value((segment.last._2, s._2)) //单位：秒
          odInterval / 1800 match {
            case 0 => extra = 600 //10min
            case 1 => extra = 900 //15min
            case _ => extra = 1200 //20min
          }
          val realInterval = abs(s._1 - segment.last._1)
          if (realInterval > odInterval + extra) {
            if (segment.length > m && segment.map(_._2).distinct.size>1) {
              segments.append(segment.toList)
            }
            segment.clear()
          }
          segment.append(s)
        }
      }//end for (s <- data)
      if (segment.length > m && segment.map(_._2).distinct.size>1) {
        segments.append(segment.toList)
      }
      (MacId, segments.toList)
    })

    //APSegments: RDD[(macId, List[List[(time, station)]])]  List[(time, station)]表示一次行程
    val filterPath = APSegments.map(line => {
      val validSegments = new ArrayBuffer[Array[(Long, String)]]()
      for (seg <- line._2) { //seg:  List[(time, station)]
        val stations = seg.map(_._2)
        val paths = validPathMap.value((stations.head, stations.last))
        var flag = true
        // 逐条有效路径比较
        for (path <- paths if flag) {
          var i = 0
          var j = 0
          while (i < stations.length & j < path.length) {
            if (stations(i) == path(j)){
              i += 1
              j += 1
            }
            else
              j += 1
          }
          if (i == stations.length)
            flag = false
        }

        if (!flag)
          validSegments.append(seg.toArray)
      }
      (line._1, validSegments.toArray)
    }).filter(_._2.nonEmpty)

    //filterPath：Array[Array[(time, station)]]
    val results = filterPath.flatMap(line => {
      for (seg <- line._2) yield { //seg: Array[(time, station)]
        val seg_new = seg.sortBy(_._1).map(x => transTimeToString(x._1) + "," + x._2 )
        (line._1 , seg_new.mkString(";"))
      }
    })

    results.toDF("mac","trip").write.mode("overwrite").parquet(s"/data/mac_trip-ap")
  }//end main

}
