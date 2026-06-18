package TraPartitioner

import DataPreprocess.GeneralFunctionSets.transTimeToTimestamp
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{lit, mean}

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, Set}


object HighFreqSegments {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("HighFreqSegments")
      .getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._
    val tau = 0.1
    val µW = 3.0

    // 读取地铁站点名和编号映射关系 "1,机场东,22.647011,113.8226476,1268036000,268"
    val stationFile = sc.textFile(s"/data/stationInfo-UTF-8.txt")
    val stationNo2NameRDD = stationFile.map(line => {
      val stationNo = line.split(',')(0)
      val stationName = line.split(',')(1)
      (stationNo.toInt, stationName)
    })
    val stationNo2Name = sc.broadcast(stationNo2NameRDD.collect().toMap)
    val stationName2No = sc.broadcast(stationNo2Name.value.map(v => (v._2,v._1)))
    // 读取所有有效路径的数据 "1 2 3 4 5 # 0 V 0.0000 12.6500"  “O ... D # 换乘次数 V 换乘时间 总时间”
    val validPathFile= sc.textFile(s"/data/allpath2.txt").map(line => {
      val fields = line.split(' ').dropRight(5) //只保留“1 2 3 4 5”
      val sou = stationNo2Name.value(fields(0).toInt)
      val des = stationNo2Name.value(fields(fields.length - 1).toInt)
      val path = fields.map(x => stationNo2Name.value(x.toInt))
      ((sou, des), path)
    }).groupByKey().mapValues(_.toArray) //RDD[((sou, des), Array[path])]
    val validPathMap = sc.broadcast(validPathFile.collect().toMap)


    //"mac","trip" 时间和站点之间逗号隔开，轨迹点之间分号隔开  在同一个站点连续采集到的多个轨迹点没有去重
    val apidN_trips = spark.read.parquet(s"/data/mac_trip-ap").rdd.map(line =>{
      val apidN = line(0).toString.toInt
      val trip: Array[(Long, String)] = line(1).toString.split(";").map(p => (transTimeToTimestamp(p.split(",")(0)),p.split(",")(1)))
      (apidN,trip)
    }).groupByKey()

    val apidN_HFsegs = apidN_trips.map(line => {
      val apidN = line._1
      val trips = line._2.toArray.sortBy(trip => trip(0)._1)
      val tripSegs = new ArrayBuffer[(String,Int, Array[Int])]() //(段，行程id，生成这个段的路径 id（由于后面会涉及到公共段，所以这里用数组表示）)
      val tripI2pathIs = mutable.Map[Int, Array[Int]]() //(行程id，满足条件的有效路径 id 集)
      val validPaths = ArrayBuffer[Array[String]]() //记录真实ap行程的有效路径，减少由于GLTC重叠行程空间判定条件（只根据ap行程的os和ds）带来导致错误
      val path2index = mutable.Map[Array[String],Int]()
      var pathIndex = 0
      trips.zipWithIndex.foreach(v =>{
        val trip = v._1
        val tripIndex = v._2
        val stations = trip.map(_._2).distinct
        val middleStas = stations.slice(1,stations.size - 1)
        var paths = validPathMap.value((stations.head, stations.last))
        if(middleStas.size > 0) //提前过滤出包含所有中间站点的路径
          paths = paths.filter(_.intersect(middleStas).size == middleStas.size)
        val paths1 = ArrayBuffer[Array[String]]() //paths中满足访问顺序的路径
        // 逐条有效路径比较
        for (path <- paths) {
          if(! path2index.contains(path))
            path2index += ((path,pathIndex))

          var i = 0
          var j = 0
          while (i < stations.length & j < path.length) {
            if (stations(i) == path(j)) {
              i += 1
              j += 1
            }
            else
              j += 1
          }
          if (i == stations.length) //找到一条有效路径有序经过WiFi行程seg中的所有站点
            paths1 += path

          pathIndex += 1
        }//end for
        validPaths ++= paths1
        //tripSegs：(段，行程id，生成这个段的路径 id（由于后面会涉及到公共段，所以这里用数组表示）)
        //flag: 是否不计算公共段， flag为true：不计算公共段； flag为false：计算公共段
        tripSegs ++= getCommonSegs(paths1.toArray, stations, path2index, true).flatMap(commonSeg =>
          getAllSegments(commonSeg._1.split("-")).map(seg => (seg, tripIndex,commonSeg._2)))
        tripI2pathIs += ((tripIndex, paths1.map(path => path2index(path)).toArray)) //(行程id，满足条件的有效路径 id 集)
      })//end trips.foreach
      //tripSegs: ArrayBuffer[(段，行程id，生成这个段的路径 id（由于涉及到公共段，这里用数组表示）)]  v._2.map(_._2).distinct.size去重是因为一个段可能是由同一个行程的不同路径生成的
      val tripSeg_tripIs_pathIs = tripSegs.toArray.groupBy(_._1).map(v => (v._1, v._2.map(_._2).distinct, v._2.flatMap(_._3).distinct)) //同一个路径只会生成一个段一次，但是当有多个行程都以这个路径作为有效路径时，pathIs就会有重复
      //对于同一路径生成的多个段，频次（行程数）一样的，只保留最长的那个  最后根据频次降序排列
      val tripSeg_tripIs_pathIs1 = tripSeg_tripIs_pathIs.flatMap(v => v._3.map(pathI => ((pathI,v._2.size),v))).//Iterable[((pathI,freq),(tripSeg,tripIs,pathIs))]
        groupBy(_._1).map(v => v._2.map(_._2).maxBy(_._1.split("-").size)).toArray.distinct.sortBy(-_._2.size)//根据频次降序排列

      /** 高频段集生成：
        *
        * 假设 n 为一 apId的行程数，如果我们希望最多考虑（n-1）个行程（作为高频段）就能使得相似性上界小于τ，1/(1+ µW(n-1))<τ，即可求得满足条件的 n
        * 如果我们希望最多考虑（n-4）个行程（作为高频段）就能使得相似性上界小于τ，4/(4+ µW(n-4))<τ，即可求得满足条件的 n
        * 当行程数 ≤ n时，必须考虑所有行程（即，所有行程的所有路径都包含在高频段集中）；
        * 当行程数 > n 时，我们最多考虑（n-1）个行程（作为高频段）就能使得相似性上界小于τ。
        *
        * 当行程数 > n 时，根据相似性上界计算高频段：
        * 遍历 tripSeg_tripIs_pathIs1，若将当前段（i）添加到高频段集中，若相似性上界≥τ，
        * 则将 段（i+1）添加到高频段集中，并检测此时的相似性上界是否小于τ，直到相似性上界小于τ为止
        *
        * 相似性上界：没有经过任何高频段的 afcId 与 当前apId的相似性上界
        * 相似性上界 = 最大重叠行程对数  / (最大重叠行程对数 + 剩余 ap 行程数 * µW)
        * 最大重叠行程对数：trips.size - 所有高频段的tripIs大小（已去重）
        * 剩余 ap 行程数：所有高频段的tripIs大小（已去重）
        * */
      val HFsegs = ArrayBuffer[String]()
      val numTrips = (1/tau - 1) / µW + 1 //行程数阈值，使得当一apId的行程数 > 这个阈值时，最多考虑（n-1）个行程（作为高频段）就能使得相似性上界小于τ
      if(trips.size <= numTrips){//行程数 ≤ numTrips时，必须考虑所有行程（即，所有行程的所有路径都包含在高频段集中）
        val pathI2seg = tripSeg_tripIs_pathIs1.flatMap(v =>v._3.map(pathI => (pathI,(v._1,v._3.size)))).//（pathI,(段，包含这个段的路径数)）
          groupBy(_._1).map(v =>(v._1,v._2.map(_._2).maxBy(_._2)._1))//一个pathI对应的多个段，保留路径数最多的一个段
        val pathIs_allTrips = tripI2pathIs.flatMap(_._2).toArray.distinct //所有行程的所有路径的集合
        HFsegs ++= pathIs_allTrips.map(pathI => pathI2seg(pathI)).distinct
      }else{//trips.size > numTrips
        var pathIs_HF = Array[Int]() //当前高频段对应的路径集
        var tripIs_HF = Array[Int]() //当前所有路径均属于 pathIs_HF 的tripIs
        HFsegs += tripSeg_tripIs_pathIs1(0)._1
        pathIs_HF = tripSeg_tripIs_pathIs1(0)._3
        tripIs_HF = tripSeg_tripIs_pathIs1(0)._2.filter(tripI => tripI2pathIs(tripI).diff(pathIs_HF).size == 0) //过滤出所有路径均属于pathIs_HF的行程id集
        var tr_ap_size = tripIs_HF.size //如果高频段对应的某个行程，其还有另一条路径不属于pathIs_HF，那么afcId可以与该路径重叠，从而导致过高估计 tr_ap_size
        var tr_ap_afc_size = trips.size - tr_ap_size
        var sim_ub = tr_ap_afc_size * 1.0 / (tr_ap_afc_size + tr_ap_size * µW)
        for(i <- 1 until tripSeg_tripIs_pathIs1.size if(sim_ub >= tau)){
          // tripSeg_tripIs_pathIs1(i)._1没有空间包含现有的任何高频段，即将tripSeg_tripIs_pathIs1(i)._1添加到高频段能带来新的行程，而不是当前tripIs_HF中已有的
          // 若tripSeg_tripIs_pathIs1(i)._1空间包含现有的任何高频段，那么tripSeg_tripIs_pathIs1(i)._2已包含在 tripIs_HF 中，将这个段添加进来后，tr_ap_size不变，相似性上界不变，还会增加ap轨迹的备份数
          if(tripSeg_tripIs_pathIs1(i)._2.diff(tripIs_HF).size > 0){
            HFsegs += tripSeg_tripIs_pathIs1(i)._1
            pathIs_HF = pathIs_HF.union(tripSeg_tripIs_pathIs1(i)._3).distinct
            tripIs_HF = tripSeg_tripIs_pathIs1(i)._2.filter(tripI => tripI2pathIs(tripI).diff(pathIs_HF).size == 0). //过滤出所有路径均属于pathIs_HF的行程id集
              union(tripIs_HF).distinct

            tr_ap_size = tripIs_HF.size
            tr_ap_afc_size = trips.size - tr_ap_size
            sim_ub = tr_ap_afc_size * 1.0 / (tr_ap_afc_size + tr_ap_size * µW)
          }
        }//end for
      }//end else //trips.size > numTrips

      (apidN, HFsegs,tripSeg_tripIs_pathIs1.map(v => v._1+":"+v._2.mkString(",")+":"+v._3.mkString(",")).mkString(";"),trips.size,validPaths)
    })//.filter(_._3.size>0) //过滤掉任何一个行程都没有任何路径有序经过的apId  这应该不会出现，因为SplitAPData已经将这样的apId过滤掉了

    apidN_HFsegs.cache()
    //<段，以该段为高频段的apId集>
    apidN_HFsegs.flatMap(line => line._2.map(HFseg => (HFseg,line._1))).
      groupByKey().map(v => (v._1, v._2.toArray.distinct.mkString(","))).toDF("HFseg","apidNset").
      write.mode("overwrite").parquet(s"/data/HFseg2apidNset")

    apidN_HFsegs.flatMap(_._5.map(path => path.map(s => stationName2No.value(s)).mkString(" "))).distinct.toDF().
      write.mode("overwrite").csv(s"/data/realValidPaths")

  }//end main

  //生成path的所有长度的子段  如1-2-3-4-5:，长度为1的子段：1-2、2-3、3-4、4-5，长度为2的子段：1-2-3、2-3-4、3-4-5，……
  def getAllSegments(path: Array[String]):Array[String]={
    val segments = ArrayBuffer[String]()
    val path2Index: Array[(String, Int)] = path.zipWithIndex

    var i = 1 //控制当前段的长度
    while(i <= path.size - 1){ //path包含的基础段数（也就是路径长度）
      for (k <- 0 to path.size - 1 - i)
        segments.append(path2Index.filter(v=>v._2>=k && v._2 <= k+i).map(_._1).mkString("-"))
      i+=1
    }
    segments.toArray
  }

  //一个行程满足条件的路径可能有多条，aim: 找到这些路径的公共段（可能不止一个）
  //基于所有ap数据的测试，只有25%的ap行程存在满足条件的路径数在 2 ~ 7，大部分为 1
  //flag: 是否不计算公共段， flag为true：不计算公共段； flag为false：计算公共段
  def getCommonSegs(paths: Array[Array[String]], stations: Array[String], path2index: mutable.Map[Array[String],Int], flag: Boolean): Array[(String, Array[Int])]={
    val commonSegs = ArrayBuffer[(String, Array[Int])]() //(seg, pathIs)

    if(paths.size < 4 || flag){//flag为true：不计算公共段； flag为false：计算公共段
      paths.foreach(path =>{
        val sIndex = path.indexOf(stations(0))
        val eIndex = path.indexOf(stations(stations.length - 1))
        val partialPath = path.zipWithIndex.filter(s => s._2 >= sIndex && s._2 <= eIndex).map(_._1)
        commonSegs += ((partialPath.mkString("-"), Array(path2index(path))))
      })
    } else {// paths1.size >= 4，找公共段
      val seg_pathIs: Array[(String, Array[Int])] = paths.flatMap(path => {
        val sIndex = path.indexOf(stations(0))
        val eIndex = path.indexOf(stations(stations.length - 1))
        val partialPath = path.zipWithIndex.filter(s => s._2 >= sIndex && s._2 <= eIndex).map(_._1)
        val segs = getAllSegments(partialPath).map(seg => (seg, path2index(path)))

      segs
      }).groupBy(_._1).toArray.flatMap(v => v._2.map(_._2).map(pathI => ((pathI, v._2.size), (v._1,v._2.map(_._2))))).//((pathI,频次：包含这个段的路径数)，(段, pathIs))
        groupBy(_._1).map(v => v._2.map(_._2).maxBy(_._1.split("-").size)).toArray.sortBy(-_._2.size) //由同一路径生成且频次一样的多个段，只保留最长的那个，并按频次降序排列（频次一样的多个段，直接保留最长的那个会导致某些路径遗漏）
      //(seg, pathIs) pathIs.size 代表的是有多少个路径包含当前这个段

      commonSegs += seg_pathIs(0)
      var existingPathIs = commonSegs.flatMap(_._2).distinct
      while(existingPathIs.size < paths.size){//得把所有路径囊括进来：每次从剩余段中选择pathIs与restPathIs交集最大的段加入到commonSegs中
        val restPathIs = paths.map(path => path2index(path)).diff(existingPathIs)
        val tmp: Array[((String, Array[Int]), Int)] = seg_pathIs.diff(commonSegs).map(seg_pathI => (seg_pathI,seg_pathI._2.intersect(restPathIs).size)).filter(_._2 > 0).
          groupBy(_._2).toArray.map(v => (v._2.map(_._1).maxBy(_._1.split("-").size), v._1)) //频次一样的，只保留最长的段（有多个频次）
        commonSegs += tmp.maxBy(_._2)._1 //取频次最大的段
        existingPathIs = commonSegs.flatMap(_._2).distinct
      }

    }//end else
    commonSegs.toArray
  }//end func

}
