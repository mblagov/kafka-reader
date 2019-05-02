package bigdata.coursetask.reader

import kafka.common.TopicAndPartition
import org.apache.curator.framework.CuratorFramework
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.kafka010.HasOffsetRanges
import org.apache.zookeeper.KeeperException.NodeExistsException

class OffsetManager(zkClient: CuratorFramework) {

  type PartitionsOffset = Map[Int, Long]

  val zkPath = "/kafka-reader"

  def init(): Unit = {
    zkClient.create()
      .creatingParentsIfNeeded()
      .forPath(zkPath)
  }

  /*def readOffsets(topic: String) = {
    val data = zkClient.getData.forPath(zkPath)
    val offsetsRangesStr = new String(data)

    offsetsRangesStr.split(",")
      .map(s => s.split(":"))
      .map { case Array(partitionStr, offsetStr) => (TopicAndPartition(topic, partitionStr.toInt) -> offsetStr.toLong) }
      .toMap
  }*/

}

