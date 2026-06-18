package TraPartitioner

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, Queue, Stack, Set}
//站点为字符串
class SegmentTree extends Serializable{
  import SegmentTree._
  val segments: mutable.Map[String, ArrayBuffer[Segment]] = mutable.Map.empty
  val root = new Segment(null)

  /** 树构建  广度优先
    * 输入：1) 所有基础段（一条线路上的两相邻站点形成一个基础段）
    *       2) 每个站点对应的邻居站点集
    * */
  def getTree(sta2nextStas: Map[String, Array[(String, String)]], //Map[(fromSta, Array[(toSta, line_direc)])] 每个站点的邻居站点集以及每个邻居站点所在线路及线路方向
              basicSeg_line_direc: Array[((String, String), Int, Int)],//Array[(基础段, line, direc)] direc = 0 或 1，代表线路方向（上行 or 下行）
              linedirec2stations: Map[String, Array[String]], //Map[(line_direc, 有序stations)]
              validPathMap: Map[(String, String), Array[String]]): this.type ={
    root.children = basicSeg_line_direc.map(_._1).map(seg => new Segment(root,seg._1, seg._2)).toArray  //所有基础段都是root的孩子节点
    val stack = Stack[Segment]()//后进先出
    stack.pushAll(root.children.reverse)

    //Map: <基础段，该段所在的地铁线路>  不考虑一个基础段有多条直达路径的情况（2018年的深圳地铁没有这种情况）
    val basicSeg2line = basicSeg_line_direc.map(v=>(v._1,v._2)).toMap
    val processedStrSegment = ArrayBuffer[String]() //避免重复扩展一个段，节省时间和空间
    while(stack.nonEmpty){ //深度优先的方式创建树
      val curr = stack.pop()
      if(segments.contains(curr.toString)) segments(curr.toString) += curr
      else segments += ((curr.toString, ArrayBuffer(curr)))

      printf("The current Segment is: "+ curr.toString+"\n")

      if(curr.numTransfer(basicSeg2line) <= 3 && !(processedStrSegment.contains(curr.toString))){ //换乘次数≤3 且没有被扩展过的段
        printf("Extending path "+ curr.toString+" working ...\n")
        val extendedSegments = extendSegment(curr,sta2nextStas,basicSeg2line,linedirec2stations,validPathMap)
        curr.children = extendedSegments //curr扩展出来的段全都作为curr的孩子
        stack.pushAll(curr.children.reverse) //先压入右节点再压入左节点

        processedStrSegment += curr.toString
        printf("Extending path done of " + processedStrSegment.size+"\n")
      }
    }

    this
  }//end function getTree

  //分别从首尾两个方向扩展一个段
  //只有扩展出来的段属于有效路径，才放到扩展结果中
  /* 问题1：如果当前段不包括有效路径中就丢弃，会导致一些包含该段的段遗失
  解决：保证newSegment.toSta的每一个（换乘）方向都要有一个段，且该段属于有效路径*/
  def extendSegment(seg: Segment,
                    sta2nextStas: Map[String, Array[(String, String)]], //Map[(sta, Array[(neighborSta, line_direc)])] 每个站点的邻居站点集以及每个邻居站点所在线路及线路方向
                    basicSeg2line: Map[(String, String), Int], //Map: <基础段，该段所在的地铁线路>  不考虑一个基础段有多条直达路径的情况（2018年的深圳地铁没有这种情况）
                    linedirec2stations: Map[String, Array[String]], //Map[(line_direc, 有序stations)]
                    validPathMap: Map[(String, String), Array[String]]): Array[Segment] ={
    val extendedSegments = ArrayBuffer[Segment]()

    /** 基于起点站 fromSta 扩展：找新的起点站,终点站保持不变 */
    //起点站的阻塞站点。如扩展 下水径-长龙，扩展起点“下水径”时，“长龙”就是阻塞站点（不能扩展到“长龙”），即不能重复已有的段
    val closedNextSta1 = if(seg.middle.size == 0) seg.toSta else seg.middle.head
    //nextStas_fromSta: Array[(neighborSta, line_direc)]
    val nextStas_fromSta = sta2nextStas(seg.fromSta).filter(_._1 != closedNextSta1) //fromSta middle toSta
    for(nextSta_linedirec <- nextStas_fromSta){
      val newSegment = new Segment(seg)
      newSegment.fromSta = nextSta_linedirec._1 //seg.fromSta的下一个站点变成新段的起点
      newSegment.middle = seg.fromSta +: seg.middle  //在原中间站点集的头部添加 原fromSta，得到新段的中间站点集
      newSegment.toSta = seg.toSta
      //只有扩展出来的段属于有效路径，才放到扩展结果中
      if(validPathMap((newSegment.fromSta,newSegment.toSta)).contains(newSegment.toString))
        extendedSegments += newSegment
      else{//但是如果当前段不包括有效路径中就丢弃，会导致一些包含该段的段遗失 所以就必须保证newSegment.fromSta的每一个(换乘)方向都要有一个段，且该段属于有效路径
      val nextLinedirecs = sta2nextStas(newSegment.fromSta).filter(_._1 != newSegment.middle.head).map(_._2)
        for(linedirec <- nextLinedirecs){
          //沿当前线路方向扩展（不考虑换乘情况） 返回结果：最短的那个属于有效路径的扩展段
          val extendedSegment = extendSeg_linedirec(newSegment,sta2nextStas,basicSeg2line,validPathMap,linedirec,false) //flag为false：扩展起点站
          if(extendedSegment != null) extendedSegments += extendedSegment
          //考虑从当前线路方向到其他线路的换乘
          val segToTransferstas = segToTransfersta_linedirec(newSegment.fromSta,linedirec,sta2nextStas,linedirec2stations)
          segToTransferstas.foreach(segTotfsta_linedirec =>{
            val newSegment1 = newSegment.copy()
            newSegment1.fromSta = segTotfsta_linedirec._1.last
            newSegment1.middle = segTotfsta_linedirec._1.dropRight(1).reverse ++ newSegment1.middle
            val extendedSegment = extendSeg_linedirec(newSegment1,sta2nextStas,basicSeg2line,validPathMap,segTotfsta_linedirec._2,false)//flag为true：扩展起点站
            if(extendedSegment != null) extendedSegments += extendedSegment
          })
        }
      }
      //只有扩展出来的段属于有效路径，才放到扩展结果中

    }//end for

    /** 基于终点站 toSta 扩展：找新的终点站,起点站保持不变 */
    //终点站的阻塞站点
    val closedNextSta2 = if(seg.middle.size == 0) seg.fromSta else seg.middle.last //fromSta middle toSta
    //nextStas_toSta: Array[(neighborSta, line_direc)]
    val nextStas_toSta = sta2nextStas(seg.toSta).filter(_._1 != closedNextSta2)
    for(nextSta_linedirec <- nextStas_toSta){
      val newSegment = new Segment(seg)
      newSegment.fromSta = seg.fromSta
      newSegment.middle = seg.middle :+ seg.toSta
      newSegment.toSta = nextSta_linedirec._1
      //只有扩展出来的段属于有效路径，才放到扩展结果中
      if(validPathMap((newSegment.fromSta,newSegment.toSta)).contains(newSegment.toString))
        extendedSegments += newSegment
      else{//但是如果当前段不包括有效路径中就丢弃，会导致一些包含该段的段遗失 所以就必须保证newSegment.toSta的每一个方向都要有一个段，且该段属于有效路径
      val nextLinedirecs = sta2nextStas(newSegment.toSta).filter(_._1 != newSegment.middle.last).map(_._2)
        for(linedirec <- nextLinedirecs){
          //沿当前线路方向扩展（不考虑换乘情况） 最短的那个属于有效路径的扩展段
          val extendedSegment = extendSeg_linedirec(newSegment,sta2nextStas,basicSeg2line,validPathMap,linedirec,true)//flag为true：扩展终点站
          if(extendedSegment != null) extendedSegments += extendedSegment
          //考虑从当前线路方向到其他线路的换乘
          val segToTransferstas = segToTransfersta_linedirec(newSegment.toSta,linedirec,sta2nextStas,linedirec2stations)
          segToTransferstas.foreach(segTotfsta_linedirec =>{
            val newSegment1 = newSegment.copy()
            newSegment1.toSta = segTotfsta_linedirec._1.last
            newSegment1.middle = newSegment1.middle ++ segTotfsta_linedirec._1.dropRight(1)
            val extendedSegment = extendSeg_linedirec(newSegment1,sta2nextStas,basicSeg2line,validPathMap,segTotfsta_linedirec._2,true)//flag为true：扩展终点站
            if(extendedSegment != null) extendedSegments += extendedSegment
          })
        }//end for
      }//end else

    }//end for

    extendedSegments.toArray
  }//end function extendSegment

  //沿指定的线路方向扩展段，一旦找到一个属于有效路径的扩展段，就返回该扩展段
  def extendSeg_linedirec(seg: Segment,
                          sta2nextStas: Map[String, Array[(String, String)]], //Map[(sta, Array[(neighborSta, line_direc)])] 每个站点的邻居站点集以及每个邻居站点所在线路及线路方向
                          basicSeg2line: Map[(String, String), Int], //Map: <基础段，该段所在的地铁线路>  不考虑一个基础段有多条直达路径的情况（2018年的深圳地铁没有这种情况）
                          validPathMap: Map[(String, String), Array[String]],
                          linedirec: String, flag: Boolean): Segment = {
    var extendedSegment: Segment = null

    if(flag){ //flag为true：扩展终点站
      val newSegment1 = seg.copy()
      var tmp = sta2nextStas(newSegment1.toSta).filter(_._2 == linedirec)
      while(!(validPathMap((newSegment1.fromSta,newSegment1.toSta)).contains(newSegment1.toString)) && tmp.size>0 && newSegment1.numTransfer(basicSeg2line)<=3){//加换乘次数判断是因为seg.toSta的下一个站可能是别的线路上的
      //nextSta_linedirec._1 的邻居站点，且该邻居站点的线路方向为：nextSta_linedirec._2 这样的邻居站点唯一
      val nnSta = tmp(0)._1
        newSegment1.middle = newSegment1.middle :+ newSegment1.toSta
        newSegment1.toSta = nnSta
        tmp = sta2nextStas(newSegment1.toSta).filter(_._2==linedirec)
      }
      if(validPathMap((newSegment1.fromSta,newSegment1.toSta)).contains(newSegment1.toString))
        extendedSegment = newSegment1
    }else{ //flag为false：扩展起点站
      val newSegment1 = seg.copy()
      var tmp = sta2nextStas(newSegment1.fromSta).filter(_._2 == linedirec)
      while(!(validPathMap((newSegment1.fromSta,newSegment1.toSta)).contains(newSegment1.toString)) && tmp.size>0 && newSegment1.numTransfer(basicSeg2line)<=3){
        //nextSta_linedirec._1 的邻居站点，且该邻居站点的线路方向为：nextSta_linedirec._2 这样的邻居站点唯一
        val nnSta = tmp(0)._1
        newSegment1.middle = newSegment1.fromSta +: newSegment1.middle
        newSegment1.fromSta = nnSta
        tmp = sta2nextStas(newSegment1.fromSta).filter(_._2 == linedirec)
      }
      if((validPathMap((newSegment1.fromSta,newSegment1.toSta)).contains(newSegment1.toString)))
        extendedSegment = newSegment1
    }

    extendedSegment
  }

  //给定站点 sta 和线路方向 linedirec，首先过滤出linedirec 方向上顺序在 sta 之后的换乘站点，然后生成从sta到这些换乘站点的段，以及记录换乘到其他线路的线路方向
  def segToTransfersta_linedirec(sta: String, linedirec: String,
                                 sta2nextStas: Map[String, Array[(String, String)]], //Map[(sta, Array[(neighborSta, line_direc)])]
                                 linedirec2stations: Map[String, Array[String]] //Map[(line_direc, 有序stations)]
                                ): Array[(List[String], String)]={
    //List[String]: 记录从 sta 沿着 linedirec 方向到 一个换乘站点的路径，String：记录 seg通过该换乘站点换乘到下一条线路的换乘方向
    val res = ArrayBuffer[(List[String], String)]()

    //过滤出 linedirec 方向上的换乘站（相当于这条线路上的所有换乘站点）
    val transferStas = sta2nextStas.toArray.filter(_._2.map(_._2.split("_")(0)).distinct.size>1).//过滤出换乘站
      map(sta_arr => (sta_arr._1,sta_arr._2.filter(_._2 == linedirec))).filter(_._2.size>0).map(_._1) //过滤出 linedirec 方向上的换乘站

    val stations = linedirec2stations(linedirec)
    val indexOfsta = stations.indexOf(sta)
    // linedirec 方向上顺序在 sta 之后的换乘站点
    val transferStas1 =  stations.slice(indexOfsta,stations.size).intersect(transferStas).diff(Array(sta))

    for(transferSta <- transferStas1){
      val indexOftfSta = stations.indexOf(transferSta)
      val seg = stations.slice(indexOfsta,indexOftfSta + 1).toList //用于记录从 sta 沿着 linedirec 方向到 transferSta的路径
      sta2nextStas(transferSta).filter(_._2.split("_")(0) != linedirec.split("_")(0)).map(_._2).foreach(linedirec1 => res += ((seg,linedirec1)) )//记录 seg到下一条线路的换乘方向
    }//end for

    res.toArray
  }

  /** 得到某个字符串段的所有后代节点（包含它自己）*/
  def descendants(strSegment: String): Array[Segment]={
    val results = ArrayBuffer[Segment]()

    //对于strSeg对应的Segment中，过滤出有孩子节点（即被扩展过）的那个Segment（每个strSeg只会被扩展一次）
    val tmp = segments(strSegment).filter(_.children.size >= 1)
    var targetSeg: Segment = null
    if(tmp.size == 0) //strSeg位于叶子节点级
      results += segments(strSegment)(0)
    else {
      targetSeg = tmp(0)
      val queue = Queue[Segment]()
      queue += targetSeg
      while(queue.nonEmpty){ //广度优先遍历树
        val curr = queue.dequeue()
        results += curr
        queue ++= curr.children
      }
    }
    results.toArray
  }

  /** 得到某个段的所有后代节点（包含它自己）*/
  def descendants(seg: Segment): Array[Segment]={
    val results = ArrayBuffer[Segment]()

    if(isLeafSegment(seg))
      results += seg
    else{
      val queue = Queue[Segment]()
      queue += seg
      while(queue.nonEmpty){ //广度优先遍历树
        val curr = queue.dequeue()
        results += curr
        queue ++= curr.children
      }
    }
    results.toArray
  }

  /** 得到包含某个段的所有段  与descendants相比，要考虑段的某个孩子没有被扩展的情况*/
  //广度优先遍历
  def overlapSegments_BFS(strSegment: String): Array[String]={
    val results = Set[String]()

    if(isLeafSegment(strSegment)) //strSeg位于叶子节点级
      results += strSegment
    else {
      //对于strSeg对应的Segment中，过滤出有孩子节点（即被扩展过）的那个Segment（每个strSeg只会被扩展一次）
      val targetSeg = segments(strSegment).filter(_.children.size >= 1)(0)
      val queue = Queue[Segment]()
      queue += targetSeg
      while(queue.nonEmpty){ //广度优先遍历树
        val curr = queue.dequeue()
        results += curr.toString
        if(!isLeafSegment(curr) && curr.children.size==0){ //curr为没有被扩展的中间段
          val curr1 = segments(curr.toString).filter(_.children.size >= 1)(0) //找到curr.toString对应的扩展过的那个段
          queue ++= curr1.children
        }
        else
          queue ++= curr.children
      }//end while
    }//end else
    results.toArray
  }//end function

  /** 得到包含某个段的所有段  与descendants相比，要考虑段的某个孩子没有被扩展的情况*/
  //深度优先遍历：包含给定段的所有段紧挨其后 方便做优化（具体见 NetworkGne 的 optimizedOverlapSegments） 这种方法非常慢，跑不出来，报错
  def overlapSegments(strSegment: String): Array[String]={
    val results = ArrayBuffer[String]()

    if(isLeafSegment(strSegment)) //strSeg位于叶子节点级
      results += strSegment
    else {
      //对于strSeg对应的Segment中，过滤出有孩子节点（即被扩展过）的那个Segment（每个strSeg只会被扩展一次）
      val targetSeg = segments(strSegment).filter(_.children.size >= 1)(0)
      val stack = Stack[Segment]()
      stack.push(targetSeg)
      while(stack.nonEmpty){
        val curr = stack.pop()
        results += curr.toString
        if(curr.children.size > 0)
          curr.children.reverse.foreach(seg => stack.push(seg)) //先压入右节点再压入左节点
        else{ //curr.children.size = 0
          if(!isLeafSegment(curr)){//curr为没有被扩展的中间段
            //找到curr.toString对应的扩展过的那个段
            val curr1 = segments(curr.toString).filter(_.children.size >= 1)(0)
            curr1.children.reverse.foreach(seg => stack.push(seg)) //先压入右节点再压入左节点
          }
        }//end else

      }//end while
    }//end outer else

    results.toArray
  }

  //从叶节点开始，计算包含每个段的所有段
  def overlapSegments(seg2num: Map[String, Int]):this.type ={
    //一个叶子段可以对应多个Segment
    val leafSegments: Array[Segment] = segments.filterKeys(strSeg=>isLeafSegment(strSeg)).values.toArray.flatMap(arr=>arr)
    leafSegments.foreach(seg => seg.overlapSegs = Array(seg2num(seg.toString)) )

    val arr = Set[Segment]()
    arr ++= leafSegments.map(_.parent)
    var flag = true
    while(arr.nonEmpty && flag){
      val tmp = arr.map(seg=>{//过滤出满足条件的段，这些段的所有孩子的重叠段均已求得
        val children = seg.children.map(child =>{
          var child1 = child
          if(!isLeafSegment(child1) && child1.children.size==0) //seg为没有被扩展的中间段
            child1 = segments(child1.toString).filter(_.children.size >= 1)(0) //找到seg.toString对应的扩展过的那个段
          child1
        })
        (seg, children)
      }).filter(_._2.map(_.overlapSegs.size).min>=1)
      tmp.foreach(v => {
        val curr = v._1
        val children = v._2
        curr.overlapSegs = Array(seg2num(curr.toString)) ++ children.flatMap(_.overlapSegs).distinct
        printf("Computating down for Segment: "+ curr.toString+"\n")

        //将curr.toString对应的所有Segment的父段都加到 arr 中，因为可能会存在一种情况：一个段只有一个孩子段，且该孩子段为没有被扩展的中间段，如果不加下面这句，这个段就不能被加到处理队列中，他的重叠段就会一直为空
        val parents = segments(curr.toString).map(_.parent).filter(p => !p.isRoot)
        arr ++= parents

      })
      if(tmp.size==0) flag = false
      arr --= tmp.map(_._1) //将计算过的段从 arr 中删掉
      printf("The number of remaining Segments: "+ arr.size+"\n")
    }//end while

    this
  }//end func

  //给定一个段，得到该段包含的所有段
  //从root的孩子节点开始，一个
  def includingSegments(seg2num: Map[String, Int]):this.type ={

    this
  }

  //判断一个段是否是叶子结点  孩子节点为空不能代表它是叶子节点，因为没有扩展过的中间层级的段也没有孩子
  def isLeafSegment(seg: Segment):Boolean={
    //是叶子节点：段seg.toString对应的所有Segment都没有被扩展过
    segments(seg.toString).filter(_.children.size >=1).size ==0
  }

  //判断一个段是否位于叶子级  孩子节点为空不能代表它是叶子节点，因为没有扩展过的中间层级的段也没有孩子
  def isLeafSegment(strSeg: String):Boolean={
    //是叶子节点：段seg.toString对应的所有Segment都没有被扩展过
    segments(strSeg).filter(_.children.size >=1).size ==0
  }

  /** 树更新：计算以某节点的为根的子树对应的总候选对数
    * 输入：以每个段为高频段的WiFi轨迹集（Map[段，WiFi轨迹集]），经过每个段的AFC轨迹集（Map[段，AFC轨迹集]） 只要经过一次就行 候选对数的计算不能用AFC客流量，因为两个月内一个乘客可能会反复经过某个段，但是该乘客被多次计数为多个设备*/
//  def update(seg2APIDset_HF: Map[String,Array[String]], //以该段为高频段的APID集
//             seg2APIDset_subHF: Map[String,Array[String]], //以该段为次高频段的APID集
//             seg2AFCIDset: Map[String,Array[String]] //访问过该段的AFCID集（某次行程以seg为有效路径）
//            ): this.type={
//    //给定一个段，得到以该段为高频段的APID集
//    for((strSeg, apIDset_HF) <- seg2APIDset_HF){
//      val tmp = segments(strSeg).filter(_.children.size >= 1) //对于strSeg对应的Segment中，过滤出有孩子节点（即被扩展过）的那个Segment（每个strSeg只会被扩展一次）
//      var targetSeg: Segment = null
//      if(tmp.size == 0) //strSeg位于叶子节点级
//        targetSeg = segments(strSeg)(0) //只选择其中一个叶子段进行分区
//      else targetSeg = tmp(0) //没扩展过的Segment不参与分区，因为已经用与它同名且扩展过的段进行分区了，所以也无需计算这种Segment的候选对数
//      targetSeg.APIDset_HF = apIDset_HF
//    }
//    //给定一个段，得到以该段为次高频段的APID集
//    for((strSeg, apIDset_subHF) <- seg2APIDset_subHF){
//      val tmp = segments(strSeg).filter(_.children.size >= 1) //对于strSeg对应的Segment中，过滤出有孩子节点（即被扩展过）的那个Segment（每个strSeg只会被扩展一次）
//      var targetSeg: Segment = null
//      if(tmp.size == 0) //strSeg位于叶子节点级
//        targetSeg = segments(strSeg)(0) //只选择其中一个叶子段进行分区
//      else targetSeg = tmp(0) //没扩展过的Segment不参与分区，因为已经用与它同名且扩展过的段进行分区了，所以也无需计算这种Segment的候选对数
//      targetSeg.APIDset_subHF = apIDset_subHF
//    }
//    //包含一个段的AFCID集：包含该段的所有段对应的AFCID集的并集
//    //虽然一个没被扩展过的段不参与分区，但是其 AFCIDset 确实应该放到其父节点的AFCIDset中，因为这应该算在候选对数中
//    for(strSeg <- seg2AFCIDset.keys){
//      val tmp = segments(strSeg).filter(_.children.size >= 1) //对于strSeg对应的Segment中，过滤出有孩子节点（即被扩展过）的那个Segment（每个strSeg只会被扩展一次）
//      var targetSeg: Segment = null
//      if(tmp.size == 0) //strSeg位于叶子节点级
//        targetSeg = segments(strSeg)(0) //只选择其中一个叶子段
//      else targetSeg = tmp(0) //找到被扩展过的那个段
//      targetSeg.AFCIDset = overlapSegments(strSeg).flatMap(seg => seg2AFCIDset(seg)).distinct
//    }

    //计算每个段为高频段时，以该段为根的子树对应的总候选对数
//    for(strSeg <- seg2APIDset_HF.keys){
//      val tmp = segments(strSeg).filter(_.children.size >= 1) //对于strSeg对应的Segment中，过滤出有孩子节点（即被扩展过）的那个Segment（每个strSeg只会被扩展一次）
//      var targetSeg: Segment = null
//      if(tmp.size == 0) //strSeg位于叶子节点级
//        targetSeg = segments(strSeg)(0) //只选择其中一个叶子段进行分区
//      else targetSeg = tmp(0) //没扩展过的Segment不参与分区，因为已经用与它同名且扩展过的段进行分区了，所以也无需计算这种Segment的候选对数
//
//      //每个段为高频段时对应的候选对数：高频段为该段的WiFi轨迹数 × 包含该段的AFC轨迹数
//      targetSeg.numCandPairs_HF_tree = descendants(targetSeg).map(seg => seg.APIDset_HF.size * seg.AFCIDset.size).sum
//    }

//    this
//  }//end func updateTree

}//end class

object SegmentTree{

class Segment(val parent: Segment) extends Serializable{
  var fromSta: String = _
  var toSta: String = _
  var middle = List[String]()  //中间站点集

  var children = Array[Segment]()
//  var APIDset_HF = Array[String]() //以该段为高频段的APID集
//  var APIDset_subHF = Array[String]() //以该段为次高频段的APID集
//  var AFCIDset = Array[String]() //包含该段的AFCID集
//  var numCandPairs_HF_tree = 0 //该段为高频段时，以该段为根的子树对应的总候选对数
//  var numCandPairs_subHF_tree =0 //该段为次高频段时，以该段为根的子树对应的总候选对数，这个需要在高频段划分之后才能计算

  var overlapSegs = Array[Int]() //包含该段的所有段集合

  def this(parent: Segment,fromStation: String, toStation: String){ //用于基础段构建
    this(parent)
    this.fromSta = fromStation
    this.toSta = toStation
  }

  def isRoot: Boolean = parent==null

  //给定一个段，计算其换乘次数
  //2018年的深圳地铁路网没有“福田口岸-福民”（基础段）这种有两条直达路线的复杂情况，所以忽略这种情况
  def numTransfer(basicSeg2line: Map[(String, String), Int]): Int={
    if(this.isRoot || this.parent.isRoot) return 0
    else{
      var count = 0
      val tmp = this.toString.split("-")
      for(i <- 0 to tmp.size-3)
        if(basicSeg2line(tmp(i),tmp(i+1)) != basicSeg2line(tmp(i+1),tmp(i+2)))
          count += 1
      return count
    }
  }//end function numTransfer

  def copy(): Segment={
    val seg = new Segment(this.parent)
    seg.fromSta = fromSta
    seg.toSta = toSta
    seg.middle = middle
    seg.children = children
//    seg.APIDset_HF = APIDset_HF
//    seg.APIDset_subHF = APIDset_subHF
//    seg.AFCIDset = AFCIDset
//    seg.numCandPairs_HF_tree = numCandPairs_HF_tree
//    seg.numCandPairs_subHF_tree = numCandPairs_subHF_tree

    seg
  }

  override def toString: String = {
    if(this.isRoot) "" //不加这句会导致val root = new Segment(null)报空指针错误（toString方法处）
    else if(this.middle.size >= 1)
      this.fromSta + "-" + this.middle.mkString("-") + "-" + this.toSta
    else
      this.fromSta + "-" + this.toSta
  }
}//end class Segment

}//end object
