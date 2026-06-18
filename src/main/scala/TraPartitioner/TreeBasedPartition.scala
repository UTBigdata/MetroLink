package TraPartitioner

import TraPartitioner.NetworkGne.getAllBasicSegments
import TraPartitioner.SegmentTree.Segment
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, Stack}

object TreeBasedPartition {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().getOrCreate()
    val sc: SparkContext = spark.sparkContext
    import spark.implicits._
    val numPartitions = 4
    val lb = 0.95
    val ub = 1.0 //ub ≥ 1，如1.05, 此时分区之间的候选对数最大相差10%

    /**计算每个高频段的权重（候选对数）*/
    val seg2num: Map[String, Int] = sc.textFile(s"/data/seg_num").map(v => {
      val tmp = v.split(",")
      (tmp(0),tmp(1).toInt)
    }).collect().toMap

    //以该段为高频段的APID集  一个apId可能对应多个高频段
    val HFseg_apIdsetRDD = spark.read.parquet(s"/data/HFseg2apidNset").//"HFseg","apidNset"
      map(line => (line(0).toString,line(1).toString.split(",").map(_.toInt)))
    val HFseg2apidNset = HFseg_apIdsetRDD.collect().toMap

    //深度优先遍历树后得到的数组
    val treeInDepthFirst: Array[String] = sc.textFile(s"/data/treeInDepthFirst").collect()

    //直接读取 NetworkGne 生成的重叠段结果
    val tmp_segN2overlapSegNs = spark.read.parquet("/data/segN2overlapSegNs").//"segN","overlapSegNs" (段编号集，逗号分隔)
      map(line => (line(0).toString.toInt, line(1).toString.split(",").map(_.toInt)) )
    val segN2overlapSegNs = sc.broadcast(tmp_segN2overlapSegNs.collect().toMap)

    //段，访问过该段的AFCID集（某次行程以该段为有效路径）
    //filter(col("seg").isInCollection(HFsegs)). //不应该过滤掉非高频段的记录，因为一个高频段的重叠段可能时候是非高频段，因为此非高频段的afcId集仍然经过了这个高频段
    val segN_afcidNset = spark.read.parquet(s"/data/segN2afcidNset").//"segN","afcidNset"
      map(line => (line(0).toString.toInt,line(1).toString.split(",").map(_.toInt)))
    val segN2afcidNset = sc.broadcast(segN_afcidNset.collect().toMap)

    val HFseg2numAPnumAFC: Map[String, (Long, Long)] = HFseg_apIdsetRDD.map(line=>{
      val seg = line._1
      val apIdset = line._2
      val ovsegNs = segN2overlapSegNs.value(seg2num(seg)).toSet //得到包含seg的所有重叠段
      //包含seg的AFCId集
      val overlapAFCIdset = ovsegNs.flatMap(ovsegN => segN2afcidNset.value.getOrElse(ovsegN, Array[Int]())) //经实测，seg2afcIdset出现key找不到是因为刷卡数据中没有访问过这些OD对
      //每个顶点的权重：候选对数（以该顶点所对应段为高频段的WiFi轨迹数 × 包含该段的AFC轨迹数）
      //val weight = apIdset.size.toLong * overlapAFCIdset.size.toLong
      (seg,(apIdset.size.toLong, overlapAFCIdset.size.toLong))
    }).collect().toMap

    //每个分区的期望候选对数 ECP_HF = 全局总候选对数/ N
    val totalNumCPs: Long = HFseg2numAPnumAFC.values.map(v => v._1 * v._2).sum
    val ECP: Double = totalNumCPs * 1.0 / (numPartitions - 5) //留下五个分区，用来装最大的五个分区切割出来的那一半
    //Array[(gId, (segs, ratio, totalWeight))]
    val gId2segsRatio: Array[(Int, (ArrayBuffer[String], Double, Long))] = partition1(ECP, ub, lb, HFseg2numAPnumAFC, treeInDepthFirst)//（HFseg，gId ）

       //求一个切割段划分到每个相关分区的apidN集
    val splitSegPid2apidNset = gId2segsRatio.map(v => (v._1, v._2._1.filter(_.contains(":")))).filter(_._2.size > 0).flatMap(v =>{
      val pId: Int = v._1
      v._2.map(seg_info => (seg_info.split(":")(0),(pId,seg_info.split(":")(1).toDouble)))
    }).groupBy(_._1).map(v => (v._1, v._2.map(_._2))).toArray.flatMap(v =>{
      val HFseg = v._1
      val pId2ratio: Array[(Int, Double)] = v._2.sortBy(_._1) //表示需要把 HFseg 中相应比例的候选轨迹对移到当前分区pId（AFC完全复制，AP移动部分）
      val numAFCs = HFseg2numAPnumAFC(HFseg)._2//包含 HFseg 的 AFC轨迹数
      val pId2apidNset = ArrayBuffer[(Int, Array[Int])]()
      var sIndex = 0
      var eIndex = 0
      for(i <- 0 until pId2ratio.size){
        val numAPs = (pId2ratio(i)._2 * ECP / numAFCs).toInt //将 HFseg 的 numAPs 个AP划分到分区 pId2ratio(0)._1
        sIndex = eIndex
        eIndex += numAPs
        if( i < pId2ratio.size - 1)
          pId2apidNset += ((pId2ratio(i)._1, HFseg2apidNset(HFseg).slice(sIndex, eIndex))) //不包括尾
        else //将剩余的所有AP划分到最后一个分区中
          pId2apidNset += ((pId2ratio(i)._1, HFseg2apidNset(HFseg).slice(sIndex, HFseg2apidNset(HFseg).size))) //不包括尾
      }
      pId2apidNset.map(v => ((HFseg, v._1), v._2))
    }).toMap

    //被切割的段后面会有 ":"，":" 后面就是数字(ratio，≤1.0)，则表示需要把该段中相应比例的候选轨迹对移到当前分区（AFC完全复制，AP移动部分）
    val rdd = sc.parallelize(gId2segsRatio)
    val rdd1 = rdd.map(line => {
      val pId = line._1
      val HFsegs = line._2._1.toSet
      val apidNafcidNs = HFsegs.map(seg_info => {
        val tmp = seg_info.split(":")
        val seg = tmp(0)
        var apidNset = HFseg2apidNset(seg)
        val ovsegNs = segN2overlapSegNs.value(seg2num(seg)).toSet //得到包含seg的所有重叠段
        val ovafcidNset = ovsegNs.flatMap(ovsegN => segN2afcidNset.value.getOrElse(ovsegN, Array[Int]())) //经过这个高频段的afcId集
        if (tmp.size > 1) //seg 为一个切割段
          apidNset = splitSegPid2apidNset((seg,pId))

        (apidNset, ovafcidNset)
      })
      (pId, apidNafcidNs.flatMap(_._1), apidNafcidNs.flatMap(_._2)) //(pId, apidNs, afcidNs)
    })
    rdd1.cache()
    val largePIDs = rdd1.map(line => (line._1, line._2.size.toLong * line._3.size.toLong)).sortBy(-_._2).map(_._1).take(5)
    val resPidStart = numPartitions - 5
    val pid2NewPid: Map[Int, Int] = largePIDs.zipWithIndex.map { case (pid, i) => pid -> (resPidStart + i) }.toMap

    // 把重分区切成两份：AP 一半去原分区，一半去新分区；AFC 集合不变
    val splitted = rdd1.flatMap { case (pId, apidNs, afcidNs) =>
        if (largePIDs.contains(pId)) {
          val newPid = pid2NewPid(pId)
          // 简单均分：按 hash 奇偶切两半（不需要稳定顺序）
          val (left, right) = apidNs.partition(a => (a.hashCode & 1) == 0)

          Seq(
            (pId, left,  afcidNs),   // 保留在原分区
            (newPid, right, afcidNs)    // 切到新的分区号
          )
        } else {
          Seq((pId, apidNs, afcidNs))     // 其它分区不变
        }
    }
    splitted.cache()

    splitted.flatMap(line => line._2.map(apidN => (apidN,line._1))).groupByKey().map(line => (line._1,line._2.mkString(","))).
      toDF("apidN","partIds").write.mode("overwrite").parquet(s"/data/apidN_partIds")
    splitted.flatMap(line => line._3.map(afcidN => (afcidN,line._1))).groupByKey().map(line => (line._1,line._2.mkString(","))).
      toDF("afcidN","partIds").write.mode("overwrite").parquet(s"/data/afcidN_partIds")

    sc.stop()
  }//end main

  def getWgt(numAPnumAFC: (Long, Long)): Long ={
    numAPnumAFC._1 * numAPnumAFC._2
  }

  //一边划分分区，一边切割大分区，一边切割下一个段以补充小分区
  def partition1(ECP: Double, ub: Double, lb: Double,
                 HFseg2numAPnumAFC: Map[String, (Long, Long)],
                 treeInDepthFirst: Array[String])={
    val gId2segsRatio = mutable.Map[Int,(ArrayBuffer[String], Double, Long)]()

    //深度优先遍历树
    val highFreqSegs = HFseg2numAPnumAFC.keys.toArray
    var HFsegs = treeInDepthFirst.intersect(highFreqSegs)//谁在前，求交集后元素先后就按照谁的顺序  segs中每个段只出现一次
    var i = 0
    var totalWeight = getWgt(HFseg2numAPnumAFC(HFsegs(0))) //当前分区的初始总权重
    gId2segsRatio += ((i,(ArrayBuffer(HFsegs(0)), totalWeight / ECP, totalWeight))) //当前分区的ratio初始化
    HFsegs = HFsegs.drop(1) //删除segs中的第一个元素，即 HFsegs(0)
    for(currSeg <- HFsegs){
      totalWeight += getWgt(HFseg2numAPnumAFC(currSeg))
      val ratio = totalWeight / ECP
      if(ratio <= ub){
        gId2segsRatio(i)._1 += currSeg
        gId2segsRatio(i) = (gId2segsRatio(i)._1, ratio, totalWeight)
      }else{ //ratio > ub  开启一个新的分区，对上一个分区（刚刚完结的这个分区）进行处理
        /** 对上一个分区（刚刚完结的这个分区）进行处理：（1）切割大分区：直接切；（2）切割 currSeg 补充当前小分区 */
        var ratio_i = gId2segsRatio(i)._2
        if(ratio_i < lb){ /** 切割 currSeg 以补充分区 i */
          gId2segsRatio(i)._1 += currSeg +":"+ (1 - ratio_i) //把currSeg中相应比例的候选轨迹对移到当前分区（AFC完全复制，AP移动部分）
          //currSeg的ratio值 > 1 - ratio_i，证明：ratio_i + ratio_currSeg > 1.05, 那么 ratio_currSeg > 1.05 - ratio_i > 1 - ratio_i
          gId2segsRatio(i) = (gId2segsRatio(i)._1, 1.0, gId2segsRatio(i)._3 + ((1 - ratio_i)*ECP).toLong)

          //开启一个新的分区，将currSeg中的剩余候选轨迹对放进去
          i += 1
          val ratio_rest = getWgt(HFseg2numAPnumAFC(currSeg))/ECP - (1 - ratio_i)
          totalWeight = (ratio_rest * ECP).toLong
          gId2segsRatio += ((i,(ArrayBuffer(currSeg + ":" + ratio_rest), ratio_rest,totalWeight)))
        }
        else if(ratio_i > ub){ /** 切割大分区：直接切*/
          val HFseg = gId2segsRatio(i)._1(0).split(":")(0) //前面的分区方法使得 ratio大于 ub 的每一个分区都只包含一个段
          gId2segsRatio(i) = (ArrayBuffer(HFseg + ":"+ 1.0),1.0, (1.0 * ECP).toLong) //把该段中相应比例的候选轨迹对移到当前分区（AFC完全复制，AP移动部分）
          ratio_i -= 1
          while(ratio_i > ub){
            i += 1
            gId2segsRatio += ((i,(ArrayBuffer(HFseg + ":"+ 1.0),1.0,(1.0 * ECP).toLong)))
            ratio_i -= 1
          }
          i += 1
          gId2segsRatio += ((i,(ArrayBuffer(HFseg + ":" + ratio_i),ratio_i,(ratio_i * ECP).toLong)))
          if(ratio_i >= lb){ //lb ≤ ratio_i ≤ ub，无需对此时的分区 i 进行处理，正常开启一个新的分区（当前段放到一个新的分区）
            i += 1
            totalWeight = getWgt(HFseg2numAPnumAFC(currSeg))
            gId2segsRatio += ((i,(ArrayBuffer(currSeg),totalWeight / ECP,totalWeight)))
          }else{ //ratio_i < lb
            totalWeight = (ratio_i * ECP).toLong + getWgt(HFseg2numAPnumAFC(currSeg))
            if(totalWeight / ECP > ub){ //切割 currSeg 以补充分区 i
              gId2segsRatio(i)._1 += currSeg +":"+ (1 - ratio_i) //把currSeg中相应比例的候选轨迹对移到当前分区（AFC完全复制，AP移动部分）
              gId2segsRatio(i) = (gId2segsRatio(i)._1, 1.0,  gId2segsRatio(i)._3 + ((1 - ratio_i) * ECP).toLong)

              //开启一个新的分区，将currSeg中的剩余候选轨迹对放进去
              i += 1
              val ratio_rest = getWgt(HFseg2numAPnumAFC(currSeg))/ECP - (1 - ratio_i)
              totalWeight = (ratio_rest * ECP).toLong
              gId2segsRatio += ((i, (ArrayBuffer(currSeg + ":" + ratio_rest),ratio_rest, totalWeight)))
            }else{
              gId2segsRatio(i)._1 += currSeg
              gId2segsRatio(i) = (gId2segsRatio(i)._1, totalWeight / ECP, totalWeight)
            }
          }
        }//end if ratio_i > ub
        else { //lb ≤ ratio_i ≤ ub，无需对分区 i 进行处理，正常开启一个新的分区（当前段放到一个新的分区）
          i += 1
          totalWeight = getWgt(HFseg2numAPnumAFC(currSeg))
          gId2segsRatio += ((i,(ArrayBuffer(currSeg),totalWeight / ECP, totalWeight)))
        }//end else lb ≤ ratio_i ≤ ub
      }//end  //ratio > ub
    }//end for
    gId2segsRatio.toArray
  }//end func

  def partition(ECP: Double, ufactor: Double, seg2weight: Map[String, Long], treeInDepthFirst: Array[String])={
    val seg2gId = mutable.Map[String, Int]()//(高频段，分区号)

    //深度优先遍历树
    var i = 0
    var totalWeight = 0L //当前分区的总权重
    val highFreqSegs = seg2weight.keys.toArray
    val segs = treeInDepthFirst.intersect(highFreqSegs)//谁在前，求交集后元素先后就按照谁的顺序

    for(currSeg <- segs){
      if(!seg2gId.contains(currSeg)){//当前段没有被划分过
        totalWeight += seg2weight(currSeg)
        val ratio = totalWeight / ECP
        if(ratio > ufactor){ //开启一个新的组（当前段放到一个新的组）
          i += 1
          totalWeight = seg2weight(currSeg)
        }
        seg2gId += ((currSeg,i))
      }

    }//end for

    seg2gId.toArray
  }


}//end object
