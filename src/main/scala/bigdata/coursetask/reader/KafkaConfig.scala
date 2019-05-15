package bigdata.coursetask.reader

import java.util.Properties
import kafka.consumer.SimpleConsumer
import org.apache.kafka.clients.consumer.KafkaConsumer

object KafkaConfig {

  val kafkaParams = Map[String, Object](
    "bootstrap.servers" -> "t510:9092",
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    "group.id" -> "use_a_separate_group_id_for_each_stream",
    "auto.offset.reset" -> "latest",
    "enable.auto.commit" -> (false: java.lang.Boolean)
  )

  val consumer = new KafkaConsumer[String, String](createConsumerConfig(config.brokerList))

  val properties = new Properties()
  properties.put("bootstrap.servers", "t510:9092")
  properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  properties.put("group.id", "use_a_separate_group_id_for_each_stream")
  properties.put("auto.offset.reset", "latest")
  val consumer = new KafkaConsumer[String, String](properties)

  val qa = consumer.partitionsFor("messages")


    .foreach(pi => {
    val topicPartition = new TopicPartition(pi.topic(), pi.partition())



  val consumer = new SimpleConsumer("t510", leader.port, 10000, 100000, clientId)
  val topicAndPartition = TopicAndPartition(topic, partitionId)
  val request = OffsetRequest(Map(topicAndPartition -> PartitionOffsetRequestInfo(time, nOffsets)))
  val offsets = consumer.getOffsetsBefore(request).partitionErrorAndOffsets(topicAndPa
}
