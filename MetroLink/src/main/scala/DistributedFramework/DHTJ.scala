package DistributedFramework

import DataPreprocess.GeneralFunctionSets.{dayOfYear_long, transTimeToTimestamp}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.SparkConf
import org.apache.spark.util.collection.OpenHashSet

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable
import it.unimi.dsi.fastutil.ints.{Int2ObjectOpenHashMap, IntOpenHashSet}
import org.apache.spark.internal.Logging

//Indexing method：MSTT-Tree
object DHTJ extends Logging{
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.kryo.registrationRequired", "true")
      .set("spark.kryo.registrator", "DistributedFramework.MyRegistrator")

    val spark = SparkSession.builder()
      .config(conf)
      .getOrCreate()

    val sc = spark.sparkContext
    import spark.implicits._

    val tau = 0.1 //Similarity threshold
    val startTime =  transTimeToTimestamp("2018-09-01 00:00:00")

    //"apidN","partIds"
    val apidN_partIds = spark.read.parquet(s"/data/apidN_partIds").
      map(line =>(line(0).toString.toInt, line(1).toString.split(",").map(_.toInt)))
    val afcidN_partIds = spark.read.parquet(s"/data/afcidN_partIds").
      map(line =>(line(0).toString.toInt, line(1).toString.split(",").map(_.toInt)))
    val apidN2partIds = sc.broadcast(apidN_partIds.collect().toMap)
    val afcidN2partIds = sc.broadcast(afcidN_partIds.collect().toMap)
    val numPartitions1 = apidN2partIds.value.values.map(v => v.max).max + 1
    val afcPartitioner = new TraPartitions(numPartitions1)
    val apPartitioner = new TraPartitions(numPartitions1)

    //用于将行程数据中站点名转换为数字
    val stationFile = sc.textFile(s"/data/stationInfo-UTF-8.txt")
    val stationName2No = stationFile.map(line => {
      val stationNo = line.split(',')(0)
      val stationName = line.split(',')(1)
      (stationName, stationNo.toInt)
    }).collect().toMap
    //    val stationNo2Name = stationName2No.map(v=>(v._2,v._1))

    //shortpath数据格式：
    //“1 2 2.6000”对应 “O ... D 最短时间” 时间的单位：min
    val readODTimeInterval = sc.textFile(s"/data/shortpath2.txt").map(line => {
      val p = line.split(' ')
      val sou = p(0).toInt
      val des = p(1).toInt
      val interval = math.ceil((p(2).toDouble*60)).toLong
      ((sou, des), interval)
    })
    val ODIntervalMap = sc.broadcast(readODTimeInterval.collect().toMap)
    //基于真实ap轨迹数据的有效路径
    val realValidPathFile = sc.textFile(s"/data/realValidPaths").map(line => {
      val path = line.split(" ").map(_.toInt)
      ((path.head,path.last),path)
    })
    val ODs = realValidPathFile.map(_._1).distinct().collect()
    // 读取所有有效路径的数据 "1 2 3 4 5 # 0 V 0.0000 12.6500"  “O ... D # 换乘次数 V 换乘时间 总时间”
    val validPathFile = sc.textFile(s"/data/allpath2.txt").map(line => {
      val tmp = line.split(' ')
      val fields = tmp.dropRight(5) //只保留“1 2 3 4 5”
      val sou = fields(0).toInt
      val des = fields(fields.length - 1).toInt
      val path = fields.map(x => x.toInt)

      ((sou, des), path)
    }).filter(line => ! ODs.contains(line._1))//RDD[((sou, des), path]
    val validPathMap: Broadcast[Map[(Int, Int), Array[Array[Int]]]] = sc.broadcast( realValidPathFile.union(validPathFile).
      groupByKey().mapValues(_.toArray).collect().toMap )

    //把OD的有效路径替换为数字
    //intO, intD, pathNs.mkString(";")
    val OD2segNs = sc.broadcast( sc.textFile("/data/OD2pathNs").map(line => {
      val tmp = line.split(',')
      ((tmp(0).toInt, tmp(1).toInt), tmp(2).split(";").map(_.toInt))
    }).collect().toMap )
    //    val OD2segNs = sc.broadcast(validPathMap.value.map(paths => (paths._1, paths._2.map(path => seg2num(path.map(stationNo2Name).mkString("-"))))))

    // 读取flow distribution "蛇口港,黄贝岭,0,0,0,259,193,173,223,350,821,903,338,114"
    val flowDistribution = sc.textFile(s"/data/flowMap").map(line => {
      val fields = line.split(",")
      val os = stationName2No(fields(0))
      val ds = stationName2No(fields(1))
      val flow: Array[Int] = fields.takeRight(12).map(_.toInt)
      ((os, ds), flow)
    })
    val flowMap = sc.broadcast(flowDistribution.collect().toMap)

    val mostViewPathFile = sc.textFile(s"/data/MostViewPath").map(line => {
      val path = line.split(",").map(station => stationName2No(station))
      val so = path.head
      val sd = path.last
      ((so, sd), path)
    })
    val mostViewPathMap = sc.broadcast(mostViewPathFile.collect().toMap)

    //"segN","overlapSegNs" (段编号，重叠段编号集) 重叠段之间逗号分隔
    val tmp_segN2overlapSegNs = spark.read.parquet("/data/segN2overlapSegNs").
      map(line => (line(0).toString.toInt, line(1).toString.split(",").map(_.toInt)) )
    val segN2overlapSegNs = sc.broadcast(tmp_segN2overlapSegNs.collect().toMap)

    /** 按 day 切分广播（多广播，按需取用） */
    //段，访问过该段的AFCID集（某次行程以该段为有效路径）
    //"segN","day","afcidN2tripIs" 多个afcidN之间以分号隔开 afcidN:多个以 segN 为有效路径的行程索引之间逗号隔开  一个afcId可能会在一天访问同一个OD多次
    val day_segN2afcidTripIs = spark.read.parquet(s"/data/segNday2afcidNsetWithTripIndex").rdd.
      map(line => (line(1).toString.toInt,(line(0).toString.toInt,line(2).toString.split(";").map(v =>
        (v.split(":")(0).toInt,v.split(":")(1).split(",").map(_.toInt)))) // (day, (segN,Array[(afcidN, Array[tripIndex])]) )
      )).groupByKey().map(line => (line._1, line._2.toMap)) //(day, Map[(segN, Array[(afcidN, Array[tripIndex])])] )
    val byDay = day_segN2afcidTripIs.collect().toMap
    val bcByDay: Map[Int, Broadcast[Map[Int, Array[(Int, Array[Int])]]]] = byDay.map { case (day, segN2afcidTripIs) => day -> sc.broadcast(segN2afcidTripIs) }

    val afcidN_patterns = spark.read.parquet(s"/data/afcidN_patterns").map(line =>{//"afcidN","patterns" List[ List[Int].mkString(",") ].mkString(";")
    val afcidN = line(0).toString.toInt
      val patterns = line(1).toString.split(";").map(_.split(",").map(_.toInt))
      (afcidN, patterns)
    })
    val afcidN2patterns: Broadcast[Map[Int, Array[Array[Int]]]] = sc.broadcast(afcidN_patterns.collect().toMap)//afcidN2patterns.value（30天数据集）: about 292.43003845214844 MB

    /**
      * AFC data: (669404508,2019-06-01 09:21:28,世界之窗,21,2019-06-01 09:31:35,深大,22)
      */
    val AFCFile = sc.textFile(s"/data/AFCtrip").map(line => {
      val fields = line.split(',')
      val id = fields(0).toInt
      val ot = transTimeToTimestamp(fields(1))
      val os = stationName2No(fields(2))
      val dt = transTimeToTimestamp(fields(4))
      val ds = stationName2No(fields(5))
      val o_day = dayOfYear_long(ot)
      val d_day = dayOfYear_long(dt)
      val day = if (o_day == d_day) o_day else 0
      (id, ((ot - startTime).toInt, os, (dt - startTime).toInt, ds, day))
    }).filter(line=>line._2._1>=0  && line._2._5 > 0 && line._2._2 != line._2._4)

    val AFCTrips: RDD[(Int, Array[(Int, Int, Int, Int)])] = AFCFile.groupByKey().map(line => {
      val trips = line._2.toArray.sortBy(_._1)

      val afc_patterns = afcidN2patterns.value.getOrElse(line._1,  Array[Array[Int]]()) //每一个元素对应一个出行模式，每个元素是属于该出行模式的行程的集合 // AFC模式提取-基于核密度估计的聚类
      val numOccasionalTrips = trips.size - afc_patterns.map(_.size).sum
      val OOD_ub = 1.0/2 * (trips.size + numOccasionalTrips)

      (line._1, trips.map(trip => (trip._1, trip._2, trip._3, trip._4)), OOD_ub/trips.size) //(id, trips, numDays, 全局上界)
    }).filter(line => line._3 >= tau). /** Pruning the AFC trajectories with the global upper bound less than tau*/
      flatMap(line => afcidN2partIds.value(line._1).map(partId =>(partId,(line._1,line._2)))).
      partitionBy(afcPartitioner).map(_._2) /** Trajectory Partitioning */

    val APFile = spark.read.parquet(s"/data/mac_trip-ap").rdd.map(line =>{
      val id = line(0).toString.toInt
      val trip = line(1).toString.split(";").map(p => (transTimeToTimestamp(p.split(",")(0)),stationName2No(p.split(",")(1))))
      val ot = trip.head._1
      val os = trip.head._2
      val dt = trip.last._1
      val ds = trip.last._2
      val o_day = dayOfYear_long(ot)
      val d_day = dayOfYear_long(dt)
      val day = if (o_day == d_day) o_day else 0
      val stations = trip.map(_._2).distinct
      val middleStas = stations.slice(1,stations.size - 1)
      (id, (((ot - startTime).toInt, os, (dt - startTime).toInt, ds), middleStas, day))
    }).filter(line => line._2._3 > 0 && line._2._1._1 >= 0 && apidN2partIds.value.contains(line._1)).
      map(line => (line._1, (line._2._1, line._2._2))) //(apidN, ((ot, os, dt, ds), middleStas))

    // 轨迹分区，为了把一个ap轨迹划分到多个分区，将每个ap记录转换为：（partId, (apidN, trips)）
    val APTrips = APFile.groupByKey().map(line => {
      val apId = line._1
      val trips = line._2.toArray.sortBy(_._1._1)
      (apId, trips) //(apidN, Array((ot, os, dt, ds), middleStas))
    }).flatMap(line => apidN2partIds.value(line._1).map(partId =>(partId,line))).
      partitionBy(apPartitioner).map(_._2) /** Trajectory Partitioning */

    val µW = 3.0

    //    join会导致内存溢出
    //    ((Iterator[(Int, Array[((Int, Int, Int, Int), Array[Int])])], Iterator[(Int, (Array[(Int, Int, Int, Int, Int)], List[List[Int]]))]) => Iterator[Nothing]) => RDD[Nothing]
    val simpairs: RDD[(Int, Int, Double)] = APTrips.zipPartitions(AFCTrips){ (apIter, afcIter)=> {
      //      val afcidN2Trips: Map[Int, Array[(Int, Int, Int, Int)]] = afcIter.toMap

      //查询性能更快，因为下面每个段每天对应的afcidN集中的每个afcidN都要查询一次afcidN2Trips
      val afcidN2Trips = new Int2ObjectOpenHashMap[Array[(Int, Int, Int, Int)]](100000, 0.7f)
      while (afcIter.hasNext) {
        val e = afcIter.next()                 // e: (afcidN, tripsArray)
        afcidN2Trips.put(e._1, e._2)
      }

      apIter.flatMap(Tw =>{ //遍历每一条WiFi轨迹，首先根据一系列剪枝技术得到其候选AFC轨迹，最后得到其匹配AFC轨迹
        //Tw：(apidN, Array[((ot, os, dt, ds), middleStas)])
        val apTrips = Tw._2.map(_._1)

        /** STT-Tree based Filtering */
        // #########  为了避免 Tuple 装箱，内部用 Long 编码 (index_ap,index_afc) ##########
        val afcidN_rawOVTripPairs1 = mutable.HashMap.empty[Int, OpenHashSet[Long]]

        // 编码/解码 (index_ap,index_afc) -> Long
        @inline def enc(index_ap: Int, index_afc: Int): Long =
          (index_ap.toLong << 32) | (index_afc & 0xffffffffL)

        apTrips.zipWithIndex.foreach { case (apTrip, index_ap) =>
          val day = dayOfYear_long(apTrip._1 + startTime)
          val segN2afcidTripIs = if(bcByDay.contains(day)) bcByDay(day).value else Map[Int, Array[(Int, Array[Int])]]()
          //OD2segNs: (OD, Array[OD的有效路径对应的唯一整数])
          OD2segNs.value((apTrip._2, apTrip._4)).foreach { segN =>
            val ovsegNs = segN2overlapSegNs.value(segN)  // Array[Int]

            ovsegNs.foreach { ovsegN =>
              val afcidN2tripIs_path = segN2afcidTripIs.getOrElse(ovsegN, Array[(Int, Array[Int])]()) ////key找不到，代表这天没有采集到afc数据（或者被活跃天数过滤之后，这天没有采集到afc数据），也就意味着没有afc行程与当前ap行程重叠
              afcidN2tripIs_path.foreach { afcidN_idxArr =>
                val afcTrips = afcidN2Trips.getOrDefault(afcidN_idxArr._1, Array[(Int, Int, Int, Int)]())

                if ((afcTrips.size > apTrips.size && afcTrips.size <= apTrips.size / tau) || (afcTrips.size <= apTrips.size && (1.0 - tau + µW * tau) * afcTrips.size >= µW * tau * apTrips.size)) {
                  //val set = afcidN_rawOVTripPairs1.getOrElseUpdate(afcidN_idxArr._1, new OpenHashSet[Long]())
                  //afcidN_idxArr._2.foreach(ov_index_afc => set.add(enc(index_ap, ov_index_afc))) //使用HashSet去重是因为一个afc行程可能会经过当前ap行程的多个路径
                  val idxArr1 = afcidN_idxArr._2.filter(index_afc => {
                    val cur_afc = afcTrips(index_afc)
                    apTrip._1 > cur_afc._1 - 600 && apTrip._3 < cur_afc._3 + 600 //过滤出具有粗略时间覆盖关系的行程对
                  })
                  if(idxArr1.size > 0) {
                    val set = afcidN_rawOVTripPairs1.getOrElseUpdate(afcidN_idxArr._1, new OpenHashSet[Long]())
                    idxArr1.foreach(ov_index_afc => set.add(enc(index_ap, ov_index_afc))) //使用HashSet去重是因为一个afc行程可能会经过当前ap行程的多个路径
                  }//end 时间过滤
                }//end 长度过滤 canAFCidNs.contains(afcidN_idxArr._1)
              }
            }
          }//end 遍历当前ap行程的每条有效路径，获取与当前apidN具有重叠行程对的afcidN
        }


        val afcs = afcidN_rawOVTripPairs1.map(afcidN_ovPairs => {
          val afcidN = afcidN_ovPairs._1
          val  ovPairs = afcidN_ovPairs._2.iterator.map(v => ((v >>> 32).toInt, v.toInt)).toArray.sortBy(v => (v._1, v._2)) //解码为Array[(index_ap, index_afc)] ovPairs必须得是有序的，因为getOVtripPairs根据行程先后进行去重
          val ovAFCTripIs = getOVtripPairs(ovPairs,afcidN2patterns.value.getOrElse(afcidN,  Array[Array[Int]]()).flatMap(x => x)).map(_._2)//第二个参数：afcidN的retularAFCTripIs
          (afcidN, ovAFCTripIs)//Array[(afcidN, ovAFCTripIs)] ovAFCTripIs为afcidN与当前apidN的重叠AFC行程
        })

        /** afcs 根据相似性上界降序排列 */
        val afcs1 = ArrayBuffer[((Int, Array[(Int, Int, Int, Int, Int)], List[List[Int]]), Double)]()
        afcs.foreach(v => {
          val afcidN = v._1 //Int型数字
          val ovAFCTripIs = v._2 //afcidN 与当前 apidN 的重叠行程对中的afc行程索引集
          val afcPatterns = afcidN2patterns.value.getOrElse(afcidN,  Array[Array[Int]]())
          //          val afcTrips = afcidN2Trips(afcidN).zipWithIndex.map(trip_index => (trip_index._1._1, trip_index._1._2, trip_index._1._3, trip_index._1._4, trip_index._2))
          val afcTrips = afcidN2Trips.get(afcidN).zipWithIndex.map(trip_index => (trip_index._1._1, trip_index._1._2, trip_index._1._3, trip_index._1._4, trip_index._2))

          //规律行程对组列表（此处只表示了afc行程）
          val num_regularTripPairs = afcPatterns.map(pattern => pattern.intersect(ovAFCTripIs).size).sum
          val numOccaTripPairs = ovAFCTripIs.size - num_regularTripPairs
          val OOD_ub1 = 1.0 / 2 * num_regularTripPairs + numOccaTripPairs
          val num_restAPTrips = math.max(0, apTrips.size - ovAFCTripIs.size)
          val num_restAFCTrips = math.max(0, afcTrips.size - ovAFCTripIs.size)
          val sim_ub1 = OOD_ub1 / (ovAFCTripIs.size + num_restAPTrips * µW + num_restAFCTrips)

          if(sim_ub1 >= tau){
            //            val afcTrips1 = afcTrips.map(trip => ((trip._1 - startTime).toInt, trip._2, (trip._3 - startTime).toInt, trip._4, trip._5))  前面已经对时间进行编码了
            afcs1 += (((afcidN, afcTrips, afcPatterns.map(_.toList).toList), sim_ub1))//((afcidN, 行程集，patterns), sim_ub1)
          }

        })
        val afcs1_1 = afcs1.sortBy(-_._2) //根据相似性上界降序排列


        /** Verification */
        if(afcs1_1.size > 0) {
          var Tf = afcs1_1(0)._1
          var sim_ub1_Tf = afcs1_1(0)._2
          var maxSim = GLTC.sim1(Tw, Tf,
            ODIntervalMap.value,
            validPathMap.value,
            mostViewPathMap.value,
            flowMap.value,
            startTime)._3
          var flag = true
          for (i <- 1 until afcs1_1.size if flag) {
            if (afcs1_1(i)._2 < maxSim) flag = false //afcs1中当前AFC轨迹的上界小于 maxSim
            else { //Tw, afcs1(i)._1成为候选对
              val sim = GLTC.sim1(Tw, afcs1_1(i)._1,
                ODIntervalMap.value,
                validPathMap.value,
                mostViewPathMap.value,
                flowMap.value,
                startTime)._3
              if (sim > maxSim) {
                Tf = afcs1_1(i)._1
                maxSim = sim
                sim_ub1_Tf = afcs1_1(i)._2
              }
            }
          } //end for

          Iterator((Tw._1, Tf._1, maxSim))
        } else {

          Iterator((Tw._1, -1, 0.0))
        }

      }) //end while(apIter.hasNext)

    }}
    simpairs.filter(_._3 >=tau).toDF("apidN","afcidN","sim").write.mode("overwrite").parquet(s"/data/MSTT-Tree_simpair")

  }//end main

  //索引树搜索的结果中，存在一个afc行程与多个ap行程重叠或一个ap行程与多个afc行程重叠，此时需要进行去重，每个ap行程只保留一个重叠afc
  //原则：当一个ap行程与多个afc行程重叠，过滤这些afc行程中作为前面ap行程和后续ap行程的重叠行程，然后在剩余的行程中，优先挑选偶尔行程作为当前ap行程的偶尔行程
  //例子：ArrayBuffer((0,3), (1,10), (2,19), (2,21), (3,21), (4,20), (4,22))
  //例子：ArrayBuffer((0,1), (1,1), (1,3), (2,4))
  //例子：ArrayBuffer((0,1), (1,7), (4,8), (4,10), (5,9), (5,11), (6,8), (6,10), (7,9), (7,11), (8,14), (9,15), (10,17), (11,18), (12,19))
  def getOVtripPairs(ovTripPairs: Array[(Int,Int)], retularAFCTripIs: Array[Int])={

    val res = ArrayBuffer[(Int, Int)]()//(index_ap, index_afc)
    val apTripI2ovAFCTripIs = ovTripPairs.groupBy(_._1).map(v => (v._1,v._2.map(_._2).sorted)).toArray.sortBy(_._1) //按照index_ap升序排列
    apTripI2ovAFCTripIs.foreach(v =>{
      val index_ap = v._1
      val ovAFCTripIs = v._2.diff(res.map(_._2))//过滤掉已经与前面的apTrip重叠的 可能会出现：一个afc行程（时间为天）与多个ap行程重叠
      var targetTripI = (-1)
      if(ovAFCTripIs.size == 1) targetTripI = ovAFCTripIs(0)
      else if(ovAFCTripIs.size > 1) {
        val ovAFCTripI_lastAP = if(res.size > 0) res.last._2 else (-1) //前一个ap行程的重叠afc行程的索引
        val ovAFCTripIs1 = ovAFCTripIs.filter(_ > ovAFCTripI_lastAP) //当前ap行程的重叠afc行程索引应该大于前一个ap行程的
        if(ovAFCTripIs1.size == 1) targetTripI = ovAFCTripIs1(0)
        else if(ovAFCTripIs1.size >1){
          //所有后续ap行程的重叠行程
          val ovAFCTripIs_subsequentAP = apTripI2ovAFCTripIs.filter(_._1> index_ap).flatMap(_._2).distinct
          val tmp = ovAFCTripIs1.diff(ovAFCTripIs_subsequentAP)//过滤掉所有后续ap行程的重叠行程
          if(tmp.size == 0) targetTripI = ovAFCTripIs1(0)
          else if(tmp.size == 1 )  targetTripI = tmp(0)
          else{//tmp.size > 1
          val occaTripIs = tmp.diff(retularAFCTripIs)
            if(occaTripIs.size>0) targetTripI = occaTripIs(0)
            else targetTripI = tmp(0)
          }
        }//end else if(ovAFCTripIs1.size >1)
      }//end else if(ovAFCTripIs.size > 1)
      if(targetTripI > (-1)) res += ((index_ap,targetTripI))
    })
    res.toArray
  }//end func

}//end object

