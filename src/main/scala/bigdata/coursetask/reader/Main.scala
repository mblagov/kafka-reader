package bigdata.coursetask.reader

import java.io.File

import kafka.utils.{ZKGroupTopicDirs, ZkUtils}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DateType, StringType, StructType, TimestampType}
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.zookeeper.ZooDefs
import org.apache.zookeeper.data.ACL

import scala.collection.JavaConversions
import scala.collection.mutable.ListBuffer

object Main {

  def main(args: Array[String]): Unit = {

    val warehouseLocation = new File("spark-warehouse").getAbsolutePath

    val spark = SparkSession
      .builder()
      .appName("KafkaConsumer")
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .config("hive.exec.dynamic.partition.mode", "nonstrict")
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._


    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "t510:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "use_a_separate_group_id_for_each_stream",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val sqlContext = spark.sqlContext
    val sc: SparkContext = spark.sparkContext
    val ssc: StreamingContext = new StreamingContext(sc, Seconds(2))
    ssc.checkpoint("checkpoint")

    val topics = Array("messages")
    val stream: InputDStream[ConsumerRecord[String, String]] =
      KafkaUtils.createDirectStream[String, String](
        ssc,
        PreferConsistent,
        Subscribe[String, String](topics, kafkaParams)
      )

    val fromOffsets = readOffsets(Seq("messages"), "1")

    val inputDStream = KafkaUtils.createDirectStream(
      ssc,
      PreferConsistent,
      Subscribe[String,String](topics, kafkaParams, fromOffsets))


    val schema = new StructType()
      .add("message", StringType)
      .add("status", StringType)
      .add("event_type", StringType)
      .add("timestamp", TimestampType)


    // где - то тут парсится json
    stream.foreachRDD { rdd =>
      //val offset = rdd.map(_.offset()).first()

      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges

      val dataSetRDD = rdd.map(_.value()).toDS()
      val data = sqlContext.read.schema(schema).json(dataSetRDD)
        .select(
          unix_timestamp($"timestamp", "yyyy-MM-dd HH:mm:ss").cast(TimestampType).as("timestamp"),
          $"status",
          $"message",
          from_unixtime(unix_timestamp($"timestamp"), "yyyy-MM-dd").cast(DateType).as("dt"),
          $"event_type")
      persistOffsets(offsetRanges, "1", storeEndOffset = true)
      data.show() //TODO удалить после введения логирования
      data.write.insertInto("svpbigdata4.messages")
    }

    ssc.start()
    ssc.awaitTermination()
  }


  val zkClientAndConnection = ZkUtils.createZkClientAndConnection("t510:2181", 30000, 30000)
  val zkUtils = new ZkUtils(zkClientAndConnection._1, zkClientAndConnection._2, false)

  def readOffsets(topics: Seq[String], groupId:String):
  Map[TopicPartition, Long] = {

    val topicPartOffsetMap = collection.mutable.HashMap.empty[TopicPartition, Long]
    val partitionMap = zkUtils.getPartitionsForTopics(topics)

    // /consumers/<groupId>/offsets/<topic>/
    partitionMap.foreach(topicPartitions => {
      val zkGroupTopicDirs = new ZKGroupTopicDirs(groupId, topicPartitions._1)
      topicPartitions._2.foreach(partition => {
        val offsetPath = zkGroupTopicDirs.consumerOffsetDir + "/" + partition

        try {
          val offsetStatTuple = zkUtils.readData(offsetPath)
          if (offsetStatTuple != null) {
            //LOGGER.info("retrieving offset details - topic: {}, partition: {}, offset: {}, node path: {}", Seq[AnyRef](topicPartitions._1, partition.toString, offsetStatTuple._1, offsetPath): _*)

            topicPartOffsetMap.put(new TopicPartition(topicPartitions._1, Integer.valueOf(partition)),
              offsetStatTuple._1.toLong)
          }

        } catch {
          case e: Exception =>
            //LOGGER.warn("retrieving offset details - no previous node exists:" + " {}, topic: {}, partition: {}, node path: {}", Seq[AnyRef](e.getMessage, topicPartitions._1, partition.toString, offsetPath): _*)

            topicPartOffsetMap.put(new TopicPartition(topicPartitions._1, Integer.valueOf(partition)), 0L)
        }
      })
    })

    topicPartOffsetMap.toMap
  }

  def persistOffsets(offsets: Seq[OffsetRange], groupId: String, storeEndOffset: Boolean): Unit = {
    offsets.foreach(or => {
      val zkGroupTopicDirs = new ZKGroupTopicDirs(groupId, or.topic)

      val acls = new ListBuffer[ACL]()
      val acl = new ACL
      acl.setId(ZooDefs.Ids.ANYONE_ID_UNSAFE)
      acl.setPerms(ZooDefs.Perms.ALL)
      acls += acl

      val offsetPath = zkGroupTopicDirs.consumerOffsetDir + "/" + or.partition
      val offsetVal = if (storeEndOffset) or.untilOffset else or.fromOffset
      zkUtils.updatePersistentPath(zkGroupTopicDirs.consumerOffsetDir + "/"
        + or.partition, offsetVal + "", JavaConversions.bufferAsJavaList(acls))

      //LOGGER.debug("persisting offset details - topic: {}, partition: {}, offset: {}, node path: {}", Seq[AnyRef](or.topic, or.partition.toString, offsetVal.toString, offsetPath): _*)
    })
  }

}
