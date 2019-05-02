package bigdata.coursetask.reader

import kafka.common.TopicAndPartition
import org.apache.curator.framework.CuratorFramework
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.kafka010.HasOffsetRanges
import org.apache.zookeeper.KeeperException.NodeExistsException

class OffsetManager(zkClient: CuratorFramework) {

  type PartitionsOffset = Map[Int, Long]

  val zkPath = "/kafka-reader/offset"

  def init(): Unit = {
    zkClient.create()
      .creatingParentsIfNeeded()
      .forPath(zkPath)
  }

  def commitKafkaOffsets(rdd: RDD[_]): Unit = {
    val offsetsRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
    val offsetsRangesStr = offsetsRanges.map(offsetRange => s"${offsetRange.partition}:${offsetRange.fromOffset}")
      .mkString(",")
    val b = offsetsRangesStr.getBytes()
    zkClient.setData.forPath(zkPath, b)

  }

  def readOffsets(topic: String) = {
    val data = zkClient.getData.forPath(zkPath)
    val offsetsRangesStr = new String(data)

    val offsets = offsetsRangesStr.split(",")
      .map(s => s.split(":"))
      .map { case Array(partitionStr, offsetStr) => (TopicAndPartition(topic, partitionStr.toInt) -> offsetStr.toLong) }
      .toMap
  }

}

