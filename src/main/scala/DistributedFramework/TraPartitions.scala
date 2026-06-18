package DistributedFramework
import org.apache.spark.Partitioner

class TraPartitions(numParts: Int) extends Partitioner{
  override def numPartitions: Int = numParts

  override def getPartition(key: Any): Int = {
    key.toString.toInt
  }

}

class afcTraPartitions(numParts: Int) extends Partitioner{
  override def numPartitions: Int = numParts

  override def getPartition(key: Any): Int = {
    key.toString.toInt
  }

}

class apTraPartitions(numParts: Int, apId2partId: Map[Int, Int]) extends Partitioner{
  override def numPartitions: Int = numParts

  override def getPartition(key: Any): Int = {
    apId2partId.getOrElse(key.toString.toInt,0)
  }

}
