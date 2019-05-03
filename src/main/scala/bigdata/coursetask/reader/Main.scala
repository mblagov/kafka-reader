package bigdata.coursetask.reader

import java.io.File

import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DateType, StringType, StructType, TimestampType}
import org.apache.spark.streaming.kafka010.ConsumerStrategies._
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}

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

    val zkClient = ZkClientBuilder.build("localhost:2181")
    val offsetManager = new OffsetManager(zkClient)
    zkClient.start()

    val offset = offsetManager.readOffsets("messages")

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "t510:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "use_a_separate_group_id_for_each_stream",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    import spark.implicits._

    val sqlContext = spark.sqlContext
    val sc: SparkContext = spark.sparkContext
    val ssc: StreamingContext = new StreamingContext(sc, Seconds(2))

    val topics = Array("messages")

    val fromOffsets = offsetManager.readOffsets("messages")
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Assign[String, String](fromOffsets.keys.toList, kafkaParams, fromOffsets)
    )

    val schema = new StructType()
      .add("message", StringType)
      .add("status", StringType)
      .add("event_type", StringType)
      .add("timestamp", TimestampType)

    stream.foreachRDD { rdd =>
      if (!rdd.isEmpty()) {

        val dataSetRDD = rdd.map(_.value()).toDS()
        val data = sqlContext.read.schema(schema).json(dataSetRDD)
          .select(
            unix_timestamp($"timestamp", "yyyy-MM-dd HH:mm:ss").cast(TimestampType).as("timestamp"),
            $"status",
            $"message",
            from_unixtime(unix_timestamp($"timestamp"), "yyyy-MM-dd").cast(DateType).as("dt"),
            $"event_type")

        data.show() //TODO удалить после введения логирования
        data.write.insertInto("svpbigdata4.messages")
        offsetManager.saveOffset(rdd)

      }
    }

    ssc.start()
    ssc.awaitTermination()
  }
}
