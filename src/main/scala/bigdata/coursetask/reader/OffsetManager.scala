package bigdata.coursetask.reader

import org.apache.curator.framework.CuratorFramework
import org.apache.kafka.common.TopicPartition
import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.kafka010.HasOffsetRanges

class OffsetManager(zkClient: CuratorFramework, logger: Logger) {

  val zkPath = "/kafka-reader"



  def saveOffset(rdd: RDD[_]) = {
    val offsetsRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
    val offsetsRangesStr = offsetsRanges.map(offsetRange => s"${offsetRange.partition}:${offsetRange.fromOffset}")
      .mkString(",")

    val b = offsetsRangesStr.getBytes()

    logger.info(s"Save offset: $offsetsRangesStr")
    zkClient.setData().forPath(zkPath, b)
  }

  def readOffsets(topic: String): Option[Map[TopicPartition, Long]] = {

    // когда неверный path, вылетает нормально документированная ошибка
    val data = Option(zkClient.getData.forPath(zkPath))

    data.map { zkData =>
      val offsetsRangesStr = new String(zkData)
      logger.info(s"Get offset: $offsetsRangesStr")
      offsetsRangesStr.split(",")
        .map(s => s.split(":"))
        .map { case Array(partitionStr, offsetStr) => new TopicPartition(topic, partitionStr.toInt) -> offsetStr.toLong }
        .toMap
    }
  }
}

