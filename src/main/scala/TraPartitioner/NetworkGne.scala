package TraPartitioner

import java.io

import TraPartitioner.SegmentTree.Segment
import org.apache.spark.sql.SparkSession

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, Set, Stack}
import org.apache.spark.broadcast.Broadcast

/**
  * （1）路径重叠树构建；
  * （2）深度优先遍历树，得到树结构的数组形式
  * （3）给定一个段，得到每个段的重叠段
  * */
object NetworkGne {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().getOrCreate()
    val sc=spark.sparkContext
    import spark.implicits._

    val line2stations = sc.textFile("/data/line_stations").map(v =>
      (v.split(";")(0), v.split(";")(1).split(","))).collectAsMap()
    val strline2int = Map("地铁五号线"->5,"地铁十一号线"->11,"地铁二号线"->2,"地铁四号线"->4,
      "地铁一号线"->1, "地铁七号线"->7,"地铁三号线"->3, "地铁九号线"->9)
    val linedirec2stations = line2stations.toArray.flatMap(v=>Array((strline2int(v._1)+"_0",v._2),(strline2int(v._1)+"_1",v._2.reverse))).toMap
    //得到整个地铁网络中所有的基础段（作为SegmentTree的子节点）
    //首先生成每条线路两个方向的基础段，之后聚合得到整个网路的
    //basicSeg2line：Array[((String, String), line, direc)] direc = 0 或 1，代表线路方向（上行 or 下行）
    val basicSeg_line_direc = line2stations.toArray.flatMap(v => getAllBasicSegments(strline2int(v._1), v._2) )
    //尽量将一条线路上相邻的两个段放在一起，不同线路之间就没办法了，顾得了跟其他线路的，就会打乱自身的
    val basicSeg_line_direc1 = basicSeg_line_direc.filter(_._3==0) ++ basicSeg_line_direc.filter(_._3==1)
//    val basicSeg2line = basicSeg_line_direc.map(v=>(v._1,v._2)).toMap
    //得到每个站点的邻居站点集以及每个邻居站点所在线路及线路方向 : Map[(fromSta, Array[(toSta, line_direc)])]
    val sta2nextStas: Map[String, Array[(String, String)]] = basicSeg_line_direc.map(v => (v._1._1,(v._1._2,v._2+"_"+v._3))).//(fromSta, (toSta, line_direc))
      groupBy(_._1).toArray.map(v=>(v._1,v._2.map(_._2))).toMap

    val stationFile = sc.textFile(s"/data/stationInfo-UTF-8.txt")
    val stationNo2Name = stationFile.map(line => {
      val stationNo = line.split(',')(0)
      val stationName = line.split(',')(1)
      (stationNo.toInt, stationName)
    }).collect.toMap

    //基于真实ap轨迹数据的有效路径  经验证，/data/realValidPaths中的所有路径都在/data/allpath2.txt中
    val realValidPathFile = sc.textFile(s"/data/realValidPaths").map(line => {
      val path = line.split(" ").map(s => stationNo2Name(s.toInt))
      ((path.head,path.last),path.mkString("-"))
    })
    val ODs = realValidPathFile.map(_._1).distinct().collect()
    // 读取所有有效路径的数据 "1 2 3 4 5 # 0 V 0.0000 12.6500"  “O ... D # 换乘次数 V 换乘时间 总时间”
    val validPathFile = sc.textFile(s"/data/allpath2.txt").map(line => {
      val tmp = line.split(' ')
      val fields = tmp.dropRight(5) //只保留“1 2 3 4 5”
      val sou = stationNo2Name(fields(0).toInt)
      val des = stationNo2Name(fields(fields.length - 1).toInt)
      val path = fields.map(x => stationNo2Name(x.toInt)).mkString("-")

      ((sou, des), path)
    }).filter(line => ! ODs.contains(line._1))//RDD[((sou, des), path]
    var validPathMap: Map[(String, String), mutable.Buffer[String]] = realValidPathFile.union(validPathFile).
      groupByKey().mapValues(_.toArray.toBuffer).collect().toMap

    /**高频段由于是裁剪出来的子段，所以有的段不属于有效路径，但是需要计算这些段的重叠段*/
    //"HFseg","apidNset"
    val HFsegs = spark.read.parquet(s"/data/HFseg2apidNset").map(line => line(0).toString).collect()
    HFsegs.foreach(seg => {
      val tmp = seg.split("-").toList
      if(validPathMap.contains((tmp.head,tmp.last)))
        validPathMap((tmp.head,tmp.last)) += seg
      else validPathMap = validPathMap ++ Map((tmp.head,tmp.last)->Array(seg).toBuffer)
    })

    val tree = new SegmentTree().getTree(sta2nextStas, basicSeg_line_direc1, linedirec2stations, validPathMap.mapValues(_.toArray.distinct).toMap)
    //首先将每个段映射为一个数字
    val seg_num = validPathMap.mapValues(_.toArray).filter(v=>v._1._1!=v._1._2).values.flatMap(v=>v).toArray.distinct.zipWithIndex
    val seg2num = seg_num.toMap //顺序会变
//    val seg_num = sc.textFile(s"/data/seg_num").map(v => {
//      val tmp = v.split(",")
//      (tmp(0),tmp(1).toInt)
//    }).collect()
//    val seg2num = seg_num.toMap

    val treeInDepthFirst: Array[String] = getTreeInDepthFirst(tree)

    //给定一个段，得到包含该段的所有段（编号）
    tree.overlapSegments(seg2num)
    val seg2overlapSegs = tree.segments.toArray.map(v =>{
      val strSeg = v._1
      val segments = v._2
      var res = Tuple2(-1,"")
      if(tree.isLeafSegment(v._1)){
        res = (seg2num(strSeg), segments(0).overlapSegs.mkString(","))
      }else{
        val targetSeg = segments.filter(_.children.size >= 1)(0)
        res = (seg2num(strSeg), targetSeg.overlapSegs.mkString(","))
      }
      res
    })

    sc.parallelize(seg_num,1).toDF().write.mode("overwrite").csv(s"/data/seg_num")

    sc.parallelize(seg2overlapSegs).toDF("segN","overlapSegNs").
      write.mode("overwrite").parquet("/data/segN2overlapSegNs")
    sc.parallelize(treeInDepthFirst).toDF().write.mode("overwrite").csv(s"/data/treeInDepthFirst")

  }//end main

  //给定一条线路：生成所有的基础段（两相邻站点构成一个基础段）
  def getAllBasicSegments(line: Int, path: Array[String]):Array[((String, String), Int,Int)]={
    val segments = ArrayBuffer[((String, String), Int,Int)]()
    for (k <- 0 to path.size - 2){ //生成上下行两个方向的段
      segments.append( ((path(k), path(k + 1)), line,0) ) //0和1代表线路方向：上行 or 下行
      segments.append( ((path(k + 1), path(k)), line,1) )
    }

    segments.toArray
  }//end func

  //深度优先遍历树，得到树结构的数组形式
  def getTreeInDepthFirst(tree: SegmentTree)={
    val treeInDepthFirst = ArrayBuffer[String]()

    //深度优先遍历树
    val stack = Stack[Segment]() //后进先出
    stack.pushAll(tree.root.children.reverse)
    while(stack.nonEmpty){
      val curr = stack.pop()
      val currSeg = curr.toString

      if(!treeInDepthFirst.contains(currSeg))
        treeInDepthFirst += currSeg

      stack.pushAll(curr.children.reverse) //先压入右节点再压入左节点
    }//end while

    treeInDepthFirst.toArray
  }

}//end object
