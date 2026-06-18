package DistributedFramework

import DataPreprocess.GeneralFunctionSets.{hourOfDay_long,dayOfYear_long}

import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.math.{max, min}

object GLTC {
  //相似性函数
  def sim(ap: (String, Array[(Long, String, Long, String, Int)]), //ap: (apId, trips)
          afc: (String, Array[(Long, String, Long, String, Int)], List[List[Int]]),//afc: (id，出行片段集合，出行模式数组(包含出行索引信息)) Array[(Long, String, Long, String, Int)]第个元素可能是day也可能是行程下标，取决于谁调用这个函数
          ODIntervalMap: Map[(String, String), Long],
          validPathMap: Map[(String, String), Array[Array[String]]],
          mostViewPathMap: Map[(String, String), Array[String]],
          flowMap: Map[(String, String), Array[Int]]
         )={
    val gama_1 = 0.3
    val gama_2 = 0.2
    val gama_3 = 0.04
    val µN = 6.0
    val µW = 3.0
    val APId = ap._1
    val AFCId = afc._1

    val AP = ap._2 //Array[(ot, os, dt, ds, day)]  该AP的所有行程的集合
    val AFC = afc._2 //Array[(ot, os, dt, ds, day 或 行程的索引（下标从0开始）)]  AFCId的所有行程的集合
    val tr_ap_afc = new ArrayBuffer[(Int, Int)]()
    val tr_ap = new ArrayBuffer[Int]()
    val tr_afc = new ArrayBuffer[Int]()
    var index_ap = 0
    var index_afc = 0
    var conflict = new ListBuffer[(Int, Int)]

    /**计算 Overlapping trip pairs（存储在tr_ap_afc）、conflicting trip pair(存储在conflict)、
            the rest AFC trips(存储在tr_afc)、the rest AP trips(存储在tr_ap)*/
    while (index_ap < AP.length && index_afc < AFC.length) {
      val cur_ap = AP(index_ap) //(ot, os, dt, ds, day)
      val cur_afc = AFC(index_afc) //(ot, os, dt, ds,行程的索引（下标从0开始）)
      if (cur_ap._3 < cur_afc._1) { //cur_ap.dt < cur_afc.ot
        tr_ap.append(index_ap)
        index_ap += 1
      }
      else if (cur_ap._1 > cur_afc._3) { //cur_ap.ot > cur_afc.dt
        tr_afc.append(index_afc)
        index_afc += 1
      }
      else if (cur_ap._1 > cur_afc._1 - 600 && cur_ap._3 < cur_afc._3 + 600) { //cur_ap.ot > cur_afc.ot-300 && cur_ap.dt < cur_afc.dt+300
        val paths_afc = validPathMap((cur_afc._2, cur_afc._4))//此处不能用字符串，因为可能会出现："132-133-134"包含"2-1"的情况
        val paths_ap = validPathMap((cur_ap._2, cur_ap._4))

        var flag = true
        for(path_ap <- paths_ap if flag){
          if(paths_afc.exists(_.containsSlice(path_ap))){ //空间重叠
            val interval1 = ODIntervalMap(cur_afc._2, cur_ap._2) //ODIntervalMap: Map[(sou, des), interval] interval: 站间时间间隔，单位：秒
            val headGap = cur_ap._1 - cur_afc._1 //cur_ap.samp_ot-cur_afc.ot
            val interval2 = ODIntervalMap(cur_ap._4, cur_afc._4)
            val endGap = cur_afc._3 - cur_ap._3 //cur_afc.dt-cur_ap.samp_dt
            if (headGap < 600 + interval1) { //interval的单位是秒，说明headGap的单位也是秒，进一步说明cur_afc.ot的单位也是秒
              flag = false
              tr_ap_afc.append((index_ap, index_afc))
            }
          }
        }
//        val paths = validPathMap((cur_afc._2, cur_afc._4)) //validPathMap: Map[(sou, des), Array[path]]
//        var flag = true
//        for (path <- paths if flag) { //查找同时包含cur_ap.os和cur_ap.ds的路径（只要找到一条即可）
//          if (path.indexOf(cur_ap._2) >= 0 && path.indexOf(cur_ap._4) > path.indexOf(cur_ap._2)) { //cur_ap._2: samp_os, cur_ap._4: samp_ds
//            val interval1 = ODIntervalMap(path.head, cur_ap._2) //ODIntervalMap: Map[(sou, des), interval] interval: 站间时间间隔，单位：秒
//            val headGap = cur_ap._1 - cur_afc._1 //cur_ap.samp_ot-cur_afc.ot
//            val interval2 = ODIntervalMap(cur_ap._4, path.last)
//            val endGap = cur_afc._3 - cur_ap._3 //cur_afc.dt-cur_ap.samp_dt
//            if (headGap < 600 + interval1) { //interval的单位是秒，说明headGap的单位也是秒，进一步说明cur_afc.ot的单位也是秒
//              flag = false
//              tr_ap_afc.append((index_ap, index_afc))
//            }
//          }
//        }
        if (flag) {
          conflict.append((index_afc, index_ap))
        }
        index_afc += 1
        index_ap += 1
      }
      else {
        conflict.append((index_afc, index_ap))
        index_afc += 1
        index_ap += 1
      }
    }
    val conflictRatio = conflict.length.toDouble / (AP.length + AFC.length)

    // key:afc_index, value:(ap_index, score)
    var OL: Map[Int, (Int, Double)] = Map()
    var afc_pattern = List[List[Int]]() //当前AFCid的属于出行模式的行程(行程索引，下标从0开始)。每一个元素(list)对应一个出行模式（list中存储的是属于该出行模式的行程）
    if(afc._3.size > 0) afc_pattern = afc._3 //出行模式已计算好，直接用
    else{ //出行模式没有计算好 当LSH调用这个方法时，就会出现这种情况
      val pairs = new ArrayBuffer[(Long, String, Long, String, Int)]()
      for (i <- AFC.indices) { //line._2 : Array[(ot, os, dt, ds, day)] 当前id的所有行程的集合
        val trip = AFC(i)
        pairs.append((trip._1, trip._2, trip._3, trip._4, i)) //(ot, os, dt, ds,行程的索引（下标从0开始）) 当前行程为该id的第i次行程，其索引为i-1
      }
      val numDays = AFC.map(_._5).distinct.size
      afc_pattern = afc_patterns_generate(pairs, numDays)
    }
    val score = new ListBuffer[Double]() //每一个元素对应一个行程对组的得分，既包括规律行程对组的，又包括偶尔行程对组的
    var Similarity = 0d
    if (conflictRatio <= 0.1) {
      if (tr_ap_afc.nonEmpty) {
        for (pair <- tr_ap_afc) { //重叠行程对
          val trip_ap = AP(pair._1) //AP：Array[(ot, os, dt, ds, day)]
          val trip_afc = AFC(pair._2) //AFC: Array[(ot, os, dt, ds,行程的索引（下标从0开始）)]  AFCId的所有行程的集合
          val ol_1 = min((trip_ap._3 - trip_ap._1).toFloat / (trip_afc._3 - trip_afc._1), 1)
          //论文中的时空覆盖长度之比变成行程时间之比
          val ot_ap = hourOfDay_long(trip_ap._1) / 2
          val flow_ap = flowMap((trip_ap._2, trip_ap._4))(ot_ap)
          val ot_afc = hourOfDay_long(trip_afc._1) / 2
          val flow_afc = flowMap((trip_afc._2, trip_afc._4))(ot_afc)
          val ol_2 = min(flow_afc.toFloat / flow_ap, 1)
          OL += (pair._2 -> (pair._1, (1 - gama_1) * ol_1 + gama_1 * ol_2))
        }
        //首先处理存在pattern的tr_ap_afc；根据afc_pattern聚合
        var index = Set[Int]() // 记录有对应pattern的tr_ap_afc中afc的index  也就是规律的重叠行程对中afc行程(索引)的集合
        for (pattern <- afc_pattern) { //afc_pattern: 每一个元素(list)对应一个出行模式（list中存储的是属于该出行模式的行程）
          val ap_seg = new ArrayBuffer[Int]() //当前出行模式下，规律的重叠行程对的ap行程(索引)集
          val group_scores = new ArrayBuffer[Double]() //当前出行模式下，规律行程对相似度得分的集合（一个出行模式对应一个规律行程对组）
          for (i <- pattern) { //每一个pattern存储的是属于该出行模式的afc行程(索引)
            if (OL.contains(i)) { //OL: key:afc_index, value:(ap_index, score)。存储重叠行程对
              index += i
              ap_seg.append(OL(i)._1)
              group_scores.append(OL(i)._2)
            }
          }
          // 计算每个group的得分  其实就是计算当前group的得分（每个pattern对应一个行程对组）
          if (ap_seg.nonEmpty) {
            val cur_afc = AFC(pattern.head) //Array[(ot, os, dt, ds,行程的索引（下标从0开始）)]  AFCId的所有行程的集合 默认选择第一个AFC行程与聚合AP行程计算agg_score
            var agg_score = 0d
            val path = mostViewPathMap((cur_afc._2, cur_afc._4))
            // 判断此group内的ap采样片段是否可以根据同一条路径聚合
            var mostLeft = path.length
            var mostRight = -1
            var belongSamePath = true
            for (i <- ap_seg if belongSamePath) {
              val ap_os = AP(i)._2
              val ap_ds = AP(i)._4
              val left = path.indexOf(ap_os)
              val right = path.indexOf(ap_ds)
              if (left >= 0 & right > left) {
                mostLeft = min(mostLeft, left)
                mostRight = max(mostRight, right)
              }
              else {
                belongSamePath = false
              }
            }

            var time_ratio = 0f
            var flow_ratio = 0f
            if (belongSamePath & mostLeft < mostRight) {
              val agg_os = path(mostLeft)
              val agg_ds = path(mostRight)
              val agg_trip_time = ODIntervalMap((agg_os, agg_ds))
              time_ratio = min(agg_trip_time.toFloat / (cur_afc._3 - cur_afc._1), 1)
              val ot = hourOfDay_long(cur_afc._1) / 2
              val flow_afc = flowMap((cur_afc._2, cur_afc._4))(ot)
              val flow_ap = flowMap((agg_os, agg_ds))(ot)
              flow_ratio = min(flow_afc.toFloat / flow_ap, 1)
            }else {
              val agg_trip = AP(ap_seg.maxBy(x => AP(x)._3 - AP(x)._1))  //选择行程花费时间最大的那个行程作为agg_trip   AP：Array[(ot, os, dt, ds, day)]
              time_ratio = min((agg_trip._3 - agg_trip._1).toFloat / (cur_afc._3 - cur_afc._1), 1)
              val ot = hourOfDay_long(cur_afc._1) / 2
              val flow_afc = flowMap((cur_afc._2, cur_afc._4))(ot)
              val flow_ap = flowMap((agg_trip._2, agg_trip._4))(ot)
              flow_ratio = min(flow_afc.toFloat / flow_ap, 1)
            }
            agg_score = (1 - gama_1) * time_ratio + gama_1 * flow_ratio

            // 衰减
            var group_score = 0d
            val sort_a = group_scores.sorted
            for (i  <- group_scores.indices) {  //1/(1+e^(0.04*x))
              group_score += (gama_2 * agg_score + (1 - gama_2) * sort_a(i) )/ (1 + Math.exp(gama_3 * i))//原分母：Math.exp(gama_3 * i) 权重设置与论文描述的完全相反
            }
            score.append(group_score)
          }
        }//for (pattern <- afc_pattern)
        // 无pattern  偶尔行程对组
        score.append(OL.filter(x => !index.contains(x._1)).map(_._2._2).sum)
      }
      //        Similarity = score.sum / (Q + P + R)
      //会出现AP的行程数比AFC的长，导致多出来的AP行程不在 tr_ap_afc、conflict、tr_ap中，这部分AP行程应该算上。
      //AFC也如此（估计的相似性上界是基于全局的，如果不把所有的剩余行程都加进来，可能会导致上界小于计算出的相似性，导致遗漏结果）
      val tr_ap_size = AP.length - (tr_ap_afc.length + conflict.length)
      val tr_afc_size = AFC.length - (tr_ap_afc.length + conflict.length)
      Similarity = score.sum / (tr_ap_afc.length + conflict.length * µN + tr_ap_size * µW + tr_afc_size)
    }
    Tuple3(AFCId,APId,Similarity)
  }//end func

  //相似性函数 站点和卡号映射为数字 使用startTimestamp将 Int型时间编号 还原为 Long型时间戳
  def sim1(ap: (Int, Array[( (Int, Int, Int, Int), Array[Int] )]), //ap: (apId, Array[(trip, middleStas)])
          afc: (Int, Array[(Int, Int, Int, Int, Int)], List[List[Int]]),//afc: (id，出行片段集合，出行模式数组(包含出行索引信息)) Array[(Long, String, Long, String, Int)]第个元素可能是day也可能是行程下标，取决于谁调用这个函数
          ODIntervalMap: Map[(Int, Int), Long],
          validPathMap: Map[(Int, Int), Array[Array[Int]]],
          mostViewPathMap: Map[(Int, Int), Array[Int]],
          flowMap: Map[(Int, Int), Array[Int]],
          startTimestamp: Long
         )={
    val gama_1 = 0.3
    val gama_2 = 0.2
    val gama_3 = 0.04
    val µN = 6.0
    val µW = 3.0
    val APId = ap._1
    val AFCId = afc._1

    //使用startTimestamp将 Int型时间编号 还原为 Long型时间戳
    val AP = ap._2.map(v => ((v._1._1 + startTimestamp,v._1._2,v._1._3 + startTimestamp,v._1._4), v._2)) //Array[((ot, os, dt, ds), middleStas)]  该AP的所有行程的集合
    val AFC = afc._2.map(v => (v._1 + startTimestamp,v._2,v._3 + startTimestamp,v._4,dayOfYear_long(v._1 + startTimestamp))) //Array[(ot, os, dt, ds, day 或 行程的索引（下标从0开始）)]  AFCId的所有行程的集合
    val tr_ap_afc = new ArrayBuffer[(Int, Int)]()
    val tr_ap = new ArrayBuffer[Int]()
    val tr_afc = new ArrayBuffer[Int]()
    var index_ap = 0
    var index_afc = 0
    var conflict = new ListBuffer[(Int, Int)]

    /**计算 Overlapping trip pairs（存储在tr_ap_afc）、conflicting trip pair(存储在conflict)、
            the rest AFC trips(存储在tr_afc)、the rest AP trips(存储在tr_ap)*/
    while (index_ap < AP.length && index_afc < AFC.length) {
      val cur_ap = AP(index_ap)._1 //AP：Array[((ot, os, dt, ds), middleStas)]
      val middleStas_cur_ap = AP(index_ap)._2
      val cur_afc = AFC(index_afc) //(ot, os, dt, ds,行程的索引（下标从0开始）)
      if (cur_ap._3 < cur_afc._1) { //cur_ap.dt < cur_afc.ot
        tr_ap.append(index_ap)
        index_ap += 1
      }
      else if (cur_ap._1 > cur_afc._3) { //cur_ap.ot > cur_afc.dt
        tr_afc.append(index_afc)
        index_afc += 1
      }
      else if (cur_ap._1 > cur_afc._1 - 600 && cur_ap._3 < cur_afc._3 + 600) { //cur_ap.ot > cur_afc.ot-300 && cur_ap.dt < cur_afc.dt+300
        val paths_afc = validPathMap((cur_afc._2, cur_afc._4))//此处不能用字符串，因为可能会出现："132-133-134"包含"2-1"的情况
        val paths_ap = validPathMap((cur_ap._2, cur_ap._4))

        var flag = true
        for(path_ap <- paths_ap if flag){
          if(paths_afc.exists(path_afc => path_afc.containsSlice(path_ap) && middleStas_cur_ap.diff(path_afc).size == 0)){ //空间重叠
            val interval1 = ODIntervalMap(cur_afc._2, cur_ap._2) //ODIntervalMap: Map[(sou, des), interval] interval: 站间时间间隔，单位：秒
            val headGap = cur_ap._1 - cur_afc._1 //cur_ap.samp_ot-cur_afc.ot
            val interval2 = ODIntervalMap(cur_ap._4, cur_afc._4)
            val endGap = cur_afc._3 - cur_ap._3 //cur_afc.dt-cur_ap.samp_dt
            if (headGap < 600 + interval1) { //interval的单位是秒，说明headGap的单位也是秒，进一步说明cur_afc.ot的单位也是秒
              flag = false
              tr_ap_afc.append((index_ap, index_afc))
            }
          }
        }
//        val paths = validPathMap((cur_afc._2, cur_afc._4)) //validPathMap: Map[(sou, des), Array[path]]
//        var flag = true
//        for (path <- paths if flag) { //查找同时包含cur_ap.os和cur_ap.ds的路径（只要找到一条即可）
//          if (path.indexOf(cur_ap._2) >= 0 && path.indexOf(cur_ap._4) > path.indexOf(cur_ap._2)) { //cur_ap._2: samp_os, cur_ap._4: samp_ds
//            val interval1 = ODIntervalMap(path.head, cur_ap._2) //ODIntervalMap: Map[(sou, des), interval] interval: 站间时间间隔，单位：秒
//            val headGap = cur_ap._1 - cur_afc._1 //cur_ap.samp_ot-cur_afc.ot
//            val interval2 = ODIntervalMap(cur_ap._4, path.last)
//            val endGap = cur_afc._3 - cur_ap._3 //cur_afc.dt-cur_ap.samp_dt
//            if (headGap < 600 + interval1) { //interval的单位是秒，说明headGap的单位也是秒，进一步说明cur_afc.ot的单位也是秒
//              flag = false
//              tr_ap_afc.append((index_ap, index_afc))
//            }
//          }
//        }
        if (flag) {
          conflict.append((index_afc, index_ap))
        }
        index_afc += 1
        index_ap += 1
      }
      else {
        conflict.append((index_afc, index_ap))
        index_afc += 1
        index_ap += 1
      }
    }
    val conflictRatio = conflict.length.toDouble / (AP.length + AFC.length)

    // key:afc_index, value:(ap_index, score)
    var OL: Map[Int, (Int, Double)] = Map()
    var afc_pattern = List[List[Int]]() //当前AFCid的属于出行模式的行程(行程索引，下标从0开始)。每一个元素(list)对应一个出行模式（list中存储的是属于该出行模式的行程）
    if(afc._3.size > 0) afc_pattern = afc._3 //出行模式已计算好，直接用
    else{ //出行模式没有计算好 当LSH调用这个方法时，就会出现这种情况
      val pairs = new ArrayBuffer[(Long, Int, Long, Int, Int)]()
      for (i <- AFC.indices) { //line._2 : Array[(ot, os, dt, ds, day)] 当前id的所有行程的集合
        val trip = AFC(i)
        pairs.append((trip._1, trip._2, trip._3, trip._4, i)) //(ot, os, dt, ds,行程的索引（下标从0开始）) 当前行程为该id的第i次行程，其索引为i-1
      }
      val numDays = AFC.map(_._5).distinct.size
      afc_pattern = afc_patterns_generate1(pairs, numDays)
    }

    val score = new ListBuffer[Double]() //每一个元素对应一个行程对组的得分，既包括规律行程对组的，又包括偶尔行程对组的
    var Similarity = 0d
    if (conflictRatio <= 0.1) {
      if (tr_ap_afc.nonEmpty) {
        for (pair <- tr_ap_afc) { //重叠行程对
          val trip_ap = AP(pair._1)._1 //AP：Array[((ot, os, dt, ds), middleStas)]
          val trip_afc = AFC(pair._2) //AFC: Array[(ot, os, dt, ds,行程的索引（下标从0开始）)]  AFCId的所有行程的集合
          val ol_1 = min((trip_ap._3 - trip_ap._1).toFloat / (trip_afc._3 - trip_afc._1), 1)
          //论文中的时空覆盖长度之比变成行程时间之比
          val ot_ap = hourOfDay_long(trip_ap._1) / 2
          val flow_ap = flowMap((trip_ap._2, trip_ap._4))(ot_ap)
          val ot_afc = hourOfDay_long(trip_afc._1) / 2
          val flow_afc = flowMap((trip_afc._2, trip_afc._4))(ot_afc)
          val ol_2 = min(flow_afc.toFloat / flow_ap, 1)
          OL += (pair._2 -> (pair._1, (1 - gama_1) * ol_1 + gama_1 * ol_2))
        }
        //首先处理存在pattern的tr_ap_afc；根据afc_pattern聚合
        var index = Set[Int]() // 记录有对应pattern的tr_ap_afc中afc的index  也就是规律的重叠行程对中afc行程(索引)的集合
        for (pattern <- afc_pattern) { //afc_pattern: 每一个元素(list)对应一个出行模式（list中存储的是属于该出行模式的行程）
          val ap_seg = new ArrayBuffer[Int]() //当前出行模式下，规律的重叠行程对的ap行程(索引)集
          val group_scores = new ArrayBuffer[Double]() //当前出行模式下，规律行程对相似度得分的集合（一个出行模式对应一个规律行程对组）
          for (i <- pattern) { //每一个pattern存储的是属于该出行模式的afc行程(索引)
            if (OL.contains(i)) { //OL: key:afc_index, value:(ap_index, score)。存储重叠行程对
              index += i
              ap_seg.append(OL(i)._1)
              group_scores.append(OL(i)._2)
            }
          }
          // 计算每个group的得分  其实就是计算当前group的得分（每个pattern对应一个行程对组）
          if (ap_seg.nonEmpty) {
            val cur_afc = AFC(pattern.head) //Array[(ot, os, dt, ds,行程的索引（下标从0开始）)]  AFCId的所有行程的集合 默认选择第一个AFC行程与聚合AP行程计算agg_score
            var agg_score = 0d
            val path = mostViewPathMap((cur_afc._2, cur_afc._4))
            // 判断此group内的ap采样片段是否可以根据同一条路径聚合
            var mostLeft = path.length
            var mostRight = -1
            var belongSamePath = true
            for (i <- ap_seg if belongSamePath) {
              val ap_os = AP(i)._1._2 //AP：Array[((ot, os, dt, ds), middleStas)]
              val ap_ds = AP(i)._1._4
              val left = path.indexOf(ap_os)
              val right = path.indexOf(ap_ds)
              if (left >= 0 & right > left) {
                mostLeft = min(mostLeft, left)
                mostRight = max(mostRight, right)
              }
              else {
                belongSamePath = false
              }
            }

            var time_ratio = 0f
            var flow_ratio = 0f
            if (belongSamePath & mostLeft < mostRight) {
              val agg_os = path(mostLeft)
              val agg_ds = path(mostRight)
              val agg_trip_time = ODIntervalMap((agg_os, agg_ds))
              time_ratio = min(agg_trip_time.toFloat / (cur_afc._3 - cur_afc._1), 1)
              val ot = hourOfDay_long(cur_afc._1) / 2
              val flow_afc = flowMap((cur_afc._2, cur_afc._4))(ot)
              val flow_ap = flowMap((agg_os, agg_ds))(ot)
              flow_ratio = min(flow_afc.toFloat / flow_ap, 1)
            }else {
              val agg_trip = AP(ap_seg.maxBy(x => AP(x)._1._3 - AP(x)._1._1))._1  //选择行程花费时间最大的那个行程作为agg_trip   AP：Array[((ot, os, dt, ds), middleStas)]
              time_ratio = min((agg_trip._3 - agg_trip._1).toFloat / (cur_afc._3 - cur_afc._1), 1)
              val ot = hourOfDay_long(cur_afc._1) / 2
              val flow_afc = flowMap((cur_afc._2, cur_afc._4))(ot)
              val flow_ap = flowMap((agg_trip._2, agg_trip._4))(ot)
              flow_ratio = min(flow_afc.toFloat / flow_ap, 1)
            }
            agg_score = (1 - gama_1) * time_ratio + gama_1 * flow_ratio

            // 衰减
            var group_score = 0d  //group_scores: 当前出行模式下，所有规律行程对相似度得分的集合
            val sort_a = group_scores.sorted //此时group_scores中相似度只有sim1，而论文中描述的是根据sim2升序排列
            for (i  <- group_scores.indices) {  //1/(1+e^(0.04*x))
              group_score += (gama_2 * agg_score + (1 - gama_2) * sort_a(i) )/ (1 + Math.exp(gama_3 * i))
            }
            score.append(group_score)
          }
        }//for (pattern <- afc_pattern)
        // 无pattern  偶尔行程对组
        score.append(OL.filter(x => !index.contains(x._1)).map(_._2._2).sum)
      }
      //        Similarity = score.sum / (Q + P + R)
      //会出现AP的行程数比AFC的长，导致多出来的AP行程不在 tr_ap_afc、conflict、tr_ap中，这部分AP行程应该算上。
      //AFC也如此（估计的相似性上界是基于全局的，如果不把所有的剩余行程都加进来，可能会导致上界小于计算出的相似性，导致遗漏结果）
      val tr_ap_size = AP.length - (tr_ap_afc.length + conflict.length)
      val tr_afc_size = AFC.length - (tr_ap_afc.length + conflict.length)
      Similarity = score.sum / (tr_ap_afc.length + conflict.length * µN + tr_ap_size * µW + tr_afc_size)
    }
    Tuple3(AFCId,APId,Similarity)
  }

}//end object
