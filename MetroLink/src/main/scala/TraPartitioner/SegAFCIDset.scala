package TraPartitioner

import DataPreprocess.GeneralFunctionSets.{dayOfYear_long, secondsOfDay, transTimeToTimestamp}

import scala.collection.mutable.ListBuffer
import scala.math.abs
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.math._

//STT-Tree Building
object SegAFCIDset {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("SegAFCIDset")
      .getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._
    val indexType = "MSTT-Tree" //MSTT-Tree or SSTT-Tree

    val seg_num = sc.textFile(s"/data/seg_num").map(v => {
      val tmp = v.split(",")
      (tmp(0),tmp(1).toInt)
    })
    val seg2num = sc.broadcast(seg_num.collect().toMap)

    // 读取地铁站点名和编号映射关系 "1,机场东,22.647011,113.8226476,1268036000,268"
    val stationFile = sc.textFile(s"/data/stationInfo-UTF-8.txt")
    val stationName2No = stationFile.map(line => {
      val stationNo = line.split(',')(0)
      val stationName = line.split(',')(1)
      (stationName, stationNo.toInt)
    }).collect().toMap
    val stationNo2Name = stationName2No.map(v=>(v._2,v._1))

    //基于真实ap轨迹数据的有效路径
    val realValidPathFile = sc.textFile(s"/data/realValidPaths").map(line => {
      val path = line.split(" ").map(s => stationNo2Name(s.toInt))
      ((path.head,path.last),path.mkString("-"))
    })
    val ODs = realValidPathFile.map(_._1).distinct().collect()
    // 读取所有有效路径的数据 "1 2 3 4 5 # 0 V 0.0000 12.6500"  “O ... D # 换乘次数 V 换乘时间 总时间”
    val validPathFile= sc.textFile(s"/data/allpath2.txt").map(line => {
      val tmp = line.split(' ')
      val fields = tmp.dropRight(5) //只保留“1 2 3 4 5”
      val sou = stationNo2Name(fields(0).toInt)
      val des = stationNo2Name(fields(fields.length - 1).toInt)
      val path = fields.map(x => stationNo2Name(x.toInt)).mkString("-")

      ((sou, des), path)
    }).filter(line => ! ODs.contains(line._1)).//RDD[((sou, des), path]
      union(realValidPathFile).groupByKey().mapValues(_.toArray)

    val validPathMap = sc.broadcast(validPathFile.collect().toMap)

    validPathFile.filter(line => line._1._1 != line._1._2).map(line => {
      val (ori,des) = line._1
      val OD = (stationName2No(ori), stationName2No(des))
      val pathNs = line._2.map(path => seg2num.value(path))
      (OD._1, OD._2, pathNs.mkString(";"))
    }).coalesce(1).toDF().write.mode("overwrite").csv("/data/OD2pathNs")

    //AFC data: (669404508,2019-06-01 09:21:28,世界之窗,21,2019-06-01 09:31:35,深大,22)
    val AFCTrips: RDD[(Int, List[(Long, String, Long, String, Int)], Int)] = sc.textFile(s"/data/AFCtrip").map(line => {
      val fields = line.split(',')
      val id = fields(0).toInt
      val ot = transTimeToTimestamp(fields(1))
      val os = fields(2)
      val dt = transTimeToTimestamp(fields(4))
      val ds = fields(5)
      val o_day = dayOfYear_long(ot) //2019-06-01 的 dayOfMonth 应该是 1
      val d_day = dayOfYear_long(dt)
      val day = if (o_day == d_day) o_day else 0
      (id, (ot, os, dt, ds, day))
    }).filter(line => line._2._1>=startTime && line._2._3<=endTime && line._2._5 > 0 && line._2._2 != line._2._4).//过滤出在指定时间范围内的数据，过滤掉不在同一天的行程，过滤掉同站进出行程；过滤出采样afcId
      groupByKey().map(line => {
      val trips = line._2.toList.sortBy(_._1) //当前id的所有行程的集合，按tap-in时间排序
      val numDays = trips.map(_._5).toSet.size
      (line._1, trips, numDays) //(id, trips, numDays)
    }).persist(StorageLevel.MEMORY_ONLY)

    //For Length-Filtering
    AFCTrips.map(line => (line._1, line._2.size)).toDF("afcidN","numTrips").write.mode("overwrite").parquet(s"/data/afcidN2Len")

    if(indexType.contains("MSTT-Tree")){
      AFCTrips.flatMap(line=>{
        val pairs = new ArrayBuffer[(Long, String, Long, String, Int, Int)]() //(ot, os, dt, ds, day,行程的索引（下标从0开始）)
        for (i <- line._2.indices) { //line._2 : Array[(ot, os, dt, ds, day)] 当前id的所有行程的集合
          val trip = line._2(i)
          pairs.append((trip._1, trip._2, trip._3, trip._4, trip._5, i)) //(ot, os, dt, ds, day, 行程的索引（下标从0开始）) 当前行程为该id的第i次行程，其索引为i-1
        }
        pairs.flatMap(trip=>{
          val paths = validPathMap.value((trip._2,trip._4)).map(path => seg2num.value(path))
          paths.map(path => ((path,trip._5),(line._1,trip._6))) //((pathN,day) (afcidN, 以path为有效路径的行程索引/下标))
        })
      }).groupByKey().map(line => {
        val afcidN2tripIs = line._2.groupBy(_._1).map(v => (v._1+":"+v._2.toArray.map(_._2).mkString(",")))//afcidN:多个以path为有效路径的行程索引之间逗号隔开
        (line._1._1, line._1._2,afcidN2tripIs.mkString(";")) //多个afcidN之间以分号隔开
      }).toDF("segN","day","afcidN2tripIs").write.mode("overwrite").parquet(s"/data/segNday2afcidNsetWithTripIndex")
    }else if(indexType.contains("SSTT-Tree")){
      /** SSTT-Tree */
      AFCTrips.flatMap(line=>{
        val pairs = new ArrayBuffer[(Long, String, Long, String, Int, Int)]() //(ot, os, dt, ds, day,行程的索引（下标从0开始）)
        for (i <- line._2.indices) { //line._2 : Array[(ot, os, dt, ds, day)] 当前id的所有行程的集合
          val trip = line._2(i)
          pairs.append((trip._1, trip._2, trip._3, trip._4, trip._5, i)) //(ot, os, dt, ds, day, 行程的索引（下标从0开始）) 当前行程为该id的第i次行程，其索引为i-1
        }
        pairs.flatMap(trip=>{
          val paths = validPathMap.value((trip._2,trip._4)).map(path => seg2num.value(path))
          paths.map(path => (path,(line._1,trip._6,trip._5))) //(pathN，(afcidN, 以path为有效路径的行程索引/下标,day: afcidN访问这个行程的日期))
        })
      }).groupByKey().map(line => {
        val afcidN2tripIdays = line._2.groupBy(_._1).map(v => (v._1+":"+v._2.toArray.map(x =>x._2+"#"+x._3).mkString(",")))//afcidN:多个以path为有效路径的行程日期之间逗号隔开（日期和行程索引之间“#”隔开）
        (line._1,afcidN2tripIdays.mkString(";")) //多个afcidN之间以分号隔开
      }).toDF("segN","afcidN2tripIdays").write.mode("overwrite").parquet(s"/data/segN_afcidN2tripIdays")
    }else{
      throw new Exception("Invalid index type input")
    }

    /**这部分的输出用于分区*/
    //<seg，访问过该段的AFCID集（某次行程以seg为有效路径）> 段和afcId都已转换为Int型数字
    val segN_afcidNset = AFCTrips.flatMap(line => {
      val afcidN = line._1
      val trips = line._2
      trips.flatMap(trip =>{
        val paths = validPathMap.value((trip._2,trip._4))
        paths.map(path => (seg2num.value(path), afcidN))
      })
    }).groupByKey().map(v => (v._1, v._2.toArray.distinct))
    segN_afcidNset.map(v=>(v._1,v._2.mkString(","))).toDF("segN","afcidNset").
      write.mode("overwrite").parquet(s"/data/segN2afcidNset")

    /**提前计算每个afcidN的patterns*/
    AFCTrips.map(line => { //过滤的原因：分区时有些 afcId 没经过任何一条高频段
      // 将每次出行的索引信息记录
      val pairs = new ArrayBuffer[(Long, String, Long, String, Int)]() //(ot, os, dt, ds,行程的索引（下标从0开始）)
      for (i <- line._2.indices) { //line._2 : Array[(ot, os, dt, ds, day)] 当前id的所有行程的集合
        val trip = line._2(i)
        pairs.append((trip._1, trip._2, trip._3, trip._4, i)) //(ot, os, dt, ds,行程的索引（下标从0开始）) 当前行程为该id的第i次行程，其索引为i-1
      }
      val numDays = line._3
      val afc_patterns: List[List[Int]] = afc_patterns_generate(pairs, numDays) // AFC模式提取-基于核密度估计的聚类
      //      val numOccasionalTrips = pairs.size - afc_patterns.map(_.size).sum
      //      val OOD_ub = 1.0/2 * (pairs.size + numOccasionalTrips)

      // id、出行片段集合、出行模式数组(包含出行索引信息)
      (line._1, afc_patterns) //,OOD_ub/pairs.size //afc_patterns中每一个元素对应一个出行模式，每个元素是属于该出行模式的行程的集合
    }).filter(_._2.size > 0)
      .map(line => (line._1, line._2.map(_.mkString(",")).mkString(";"))).
      toDF("afcidN","patterns").write.mode("overwrite").parquet(s"/data/afcidN_patterns")

  }//end main

  case class distAndKinds(var d: Long, var k: Int)

  // 高斯核函数
  def RBF(l: Long, x: Long, h: Int): Double = {
    1 / sqrt(2 * Pi) * exp(-pow(x - l, 2) / (2 * pow(h, 2)))
  }

  // 计算相对距离
  def compute_dist(info: Array[(Double, Long)]): Array[Long] = {
    val result = new Array[Long](info.length)
    val s = mutable.Stack[Int]()
    s.push(0)
    var i = 1
    var index = 0
    while (i < info.length) {
      if (s.nonEmpty && info(i)._1 > info(s.top)._1) {
        index = s.pop()
        result(index) = abs(info(i)._2 - info(index)._2)
      }
      else {
        s.push(i)
        i += 1
      }
    }
    while (s.nonEmpty) {
      result(s.pop()) = -1
    }
    result
  }

  // 计算z_score自动选取聚类中心
  def z_score(dens_pos: Array[(Double, Long)]): Array[(Int, Long)] = {
    val dist_r = compute_dist(dens_pos)
    val dist_l = compute_dist(dens_pos.reverse).reverse
    val dist_dens_pos = new ArrayBuffer[(Long, Double, Long)]()
    for (i <- dist_r.indices) {
      if (dist_r(i) == -1 && dist_l(i) == -1)
        dist_dens_pos.append((dens_pos.last._2 - dens_pos.head._2, dens_pos(i)._1, dens_pos(i)._2))
      else if (dist_r(i) != -1 && dist_l(i) != -1)
        dist_dens_pos.append((min(dist_r(i), dist_l(i)), dens_pos(i)._1, dens_pos(i)._2))
      else if (dist_l(i) != -1)
        dist_dens_pos.append((dist_l(i), dens_pos(i)._1, dens_pos(i)._2))
      else
        dist_dens_pos.append((dist_r(i), dens_pos(i)._1, dens_pos(i)._2))
    }
    var sum_dist = 0L
    var sum_dens = 0d
    dist_dens_pos.foreach(x => {
      sum_dist += x._1
      sum_dens += x._2
    })
    val avg_dist = sum_dist / dist_dens_pos.length
    val avg_dens = sum_dens / dist_dens_pos.length
    var total = 0d
    for (v <- dist_dens_pos) {
      total += pow(abs(v._1 - avg_dist), 2) + pow(abs(v._2 - avg_dens), 2)
    }
    val sd = sqrt(total / dist_dens_pos.length)
    val z_score = new ArrayBuffer[((Long, Double, Long), Double)]()
    var z_value = 0d
    for (v <- dist_dens_pos) {
      z_value = sqrt(pow(abs(v._1 - avg_dist), 2) + pow(abs(v._2 - avg_dens), 2)) / sd
      z_score.append((v, z_value))
    }
    val result = new ArrayBuffer[(Int, Long)]()
    // z-score大于3认为是类簇中心
    val clustersInfo = z_score.toArray.filter(_._2 >= 3)
    for (i <- clustersInfo.indices) {
      result.append((i + 1, clustersInfo(i)._1._3))
    }
    result.toArray
  }

  def afc_patterns_generate(pairs: ArrayBuffer[(Long, String, Long, String, Int)], numDays: Int)={
    // 提取时间戳对应当天的秒数用于聚类
    val stampBuffer = new ArrayBuffer[Long]()
    pairs.foreach(v => {
      stampBuffer.append(secondsOfDay(v._1)) //ot
      stampBuffer.append(secondsOfDay(v._3)) //dt
    })
    val timestamps = stampBuffer.toArray.sorted
    // 设置带宽h，单位为秒
    val h = 1800
    // 计算局部密度
    val density_stamp_Buffer = new ArrayBuffer[(Double, Long)]()
    for (t <- timestamps) {
      var temp = 0D //给temp赋值浮点数0
      for (v <- timestamps) {
        temp += RBF(v, t, h)
      }
      density_stamp_Buffer.append((temp / (timestamps.length * h), t))
    }
    val density_stamp = density_stamp_Buffer.toArray.sortBy(_._2)

    // 判断是否存在聚类中心，若返回为空则不存在，否则分类
    val cluster_center: Array[(Int, Long)] = z_score(density_stamp)  //类别，时间戳（单位：秒）

    // 设置类边界距离并按照聚类中心分配数据
    val dc = 5400
    // 初始化类簇,结构为[所属类，出行片段] ME: 这里的出行片段也就是行程
    val clusters = new ArrayBuffer[(Int, (Long, String, Long, String, Int))]
    for (v <- pairs) { //
      if (cluster_center.nonEmpty) {
        val o_stamp = secondsOfDay(v._1)
        val d_stamp = secondsOfDay(v._3)
        val o_to_c = distAndKinds(Long.MaxValue, 0)
        val d_to_c = distAndKinds(Long.MaxValue, 0)
        for (c <- cluster_center) {
          if (abs(o_stamp - c._2) < dc && abs(o_stamp - c._2) < o_to_c.d) {
            o_to_c.k = c._1
            o_to_c.d = abs(o_stamp - c._2)
          }
          if (abs(d_stamp - c._2) < dc && abs(d_stamp - c._2) < d_to_c.d) {
            d_to_c.k = c._1
            d_to_c.d = abs(d_stamp - c._2)
          }
        }
        if (o_to_c.k == d_to_c.k && o_to_c.k != 0)
          clusters.append((o_to_c.k, v))
        else
          clusters.append((0, v))
      }
      else
        clusters.append((0, v))
    }

    // 存储所有pattern的出行索引信息
    val afc_patterns = new ListBuffer[List[Int]]()

    // 按照所属类别分组  clusters: ArrayBuffer[(类别, (ot, os, dt, ds,行程的索引（下标从0开始）) )]
    // grouped: Array[( 类别, ArrayBuffer[(类别, (ot, os, dt, ds,行程的索引（下标从0开始）) )] )]
    val grouped= clusters.groupBy(_._1).toArray.filter(x => x._1 > 0)
    if (grouped.nonEmpty) {
      grouped.foreach(g => {
        // 同一类中数据按照进出站分组   g._2: ArrayBuffer[(类别, (ot, os, dt, ds,行程的索引（下标从0开始）) )]
        //temp_data: Map[ (sou, des), Array[(类别, (ot, os, dt, ds,行程的索引（下标从0开始）) )] ]
        val temp_data= g._2.toArray.groupBy(x => (x._2._2, x._2._4)) //按照(os, ds)分组
        temp_data.foreach(v => { // v: ( (sou, des), Array[(类别, (ot, os, dt, ds,行程的索引（下标从0开始）) )] )
          // 超过总出行天数的1/2则视为出行模式  ME: 该类别中访问该od的行程数超过总出行天数的1/2则视为出行模式
          if (v._2.length >= 5 || v._2.length > numDays / 2) {
            // 存储当前pattern中所有出行(行程)的索引信息
            val temp_patterns = new ListBuffer[Int]()
            v._2.foreach(x => temp_patterns.append(x._2._5))//x._2._5: 行程的索引（下标从0开始）
            afc_patterns.append(temp_patterns.toList) //afc_patterns中每一个元素对应一个出行模式，每个元素是属于该出行模式的行程的集合
          }
        })
      })
    }

    afc_patterns.toList
  }//end func

}
