package bigdata.coursetask.reader

import org.apache.curator.framework.CuratorFramework
import org.apache.kafka.common.TopicPartition
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.kafka010.HasOffsetRanges

class OffsetManager(zkClient: CuratorFramework) {

  val zkPath = "/kafka-reader"



  def saveOffset(rdd: RDD[_]) = {
    val offsetsRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
    val offsetsRangesStr = offsetsRanges.map(offsetRange => s"${offsetRange.partition}:${offsetRange.fromOffset}")
      .mkString(",")

    val b = offsetsRangesStr.getBytes()

    zkClient.setData().forPath( "/kafka-reader", b)
  }

  def readOffsets(topic: String): Map[TopicPartition, Long] = {
    val data = zkClient.getData.forPath(zkPath)
    val offsetsRangesStr = new String(data)

    offsetsRangesStr.split(",")
      .map(s => s.split(":"))
      .map { case Array(partitionStr, offsetStr) => new TopicPartition(topic, partitionStr.toInt) -> offsetStr.toLong }
      .toMap
  }
}

