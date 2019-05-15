package bigdata.coursetask.reader

import java.io.File

import kafka.utils.ZkUtils
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DateType, StringType, StructType, TimestampType}
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}


object Main extends Logging {

  def main(args: Array[String]): Unit = {

    val warehouseLocation = new File("spark-warehouse").getAbsolutePath

    log.debug("Starting new spark session")
    val spark = SparkSession
      .builder()
      .appName("KafkaConsumer")
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .config("hive.exec.dynamic.partition.mode", "nonstrict")
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._
    log.debug("Spark session started")

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "t510:9092",
      "key.deserializer" → classOf[StringDeserializer],
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

    val schema = new StructType()
      .add("message", StringType)
      .add("status", StringType)
      .add("event_type", StringType)
      .add("timestamp", TimestampType)


    // где - то тут парсится json


    log.debug("Writing message and its offset started")
    stream.foreachRDD { rdd =>
      if(!rdd.isEmpty()) {
        log.debug("Taking offset")
        val offset = rdd.map(_.offset()).first()
        log.info(s"Took offset: $offset")
        log.debug("Converting to dataset")
        val dataSetRDD = rdd.map(_.value()).toDS()
        log.debug("Taking message from sparkContext and parsing it")
        val data = sqlContext.read.schema(schema).json(dataSetRDD)
          .select(
            unix_timestamp($"timestamp", "yyyy-MM-dd HH:mm:ss").cast(TimestampType).as("timestamp"),
            $"status",
            $"message",
            from_unixtime(unix_timestamp($"timestamp"), "yyyy-MM-dd").cast(DateType).as("dt"),
            $"event_type")
        val tableName = "svpbigdata4.messages"
        data.show()
        data.write.insertInto(tableName)
        log.info(s"Wrote message: $data into $tableName")
      } else {
        log.info("Empty collection!")
      }
    }
    log.debug("Writing message and its offset finished")

    log.debug("Starting streaming context")
    ssc.start()
    log.debug("Streaming context started")
    ssc.awaitTermination()
    log.debug("Streaming context finished")
  }

}
