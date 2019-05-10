package bigdata.coursetask.reader

import java.io.File

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DateType, StringType, StructType, TimestampType}
import org.apache.spark.streaming.kafka010.ConsumerStrategies._
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}

object Main {

  val logger: Logger = Logger.getLogger(Main.getClass)

  def main(args: Array[String]): Unit = {
    logger.setLevel(Level.DEBUG)

    val warehouseLocation = new File("spark-warehouse").getAbsolutePath
    logger.debug("Starting new spark session")
    val spark = SparkSession
      .builder()
      .appName("KafkaConsumer")
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .config("hive.exec.dynamic.partition.mode", "nonstrict")
      .enableHiveSupport()
      .getOrCreate()

    logger.debug("Spark session started")

    val zkClient = ZkClientBuilder.build("localhost:2181")
    val offsetManager = new OffsetManager(zkClient, logger)
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

    val stream = fromOffsets match {
        //нашли оффсет - пошли с него
      case Some(offset) =>
        KafkaUtils.createDirectStream[String, String](
        ssc,
        PreferConsistent,
        Assign[String, String](offset.keys.toList, kafkaParams, offset))
        //ничего не лежит в zk папке - пошли с начала
      case None =>
        KafkaUtils.createDirectStream[String, String](
          ssc,
          PreferConsistent,
          Subscribe[String, String](topics, kafkaParams)
        )
    }


    val schema = new StructType()
      .add("message", StringType)
      .add("status", StringType)
      .add("event_type", StringType)
      .add("timestamp", TimestampType)

    logger.debug("Writing message and its offset started")

    stream.foreachRDD { rdd =>
      if (!rdd.isEmpty()) {
        logger.debug("Converting to dataset")
        val dataSetRDD = rdd.map(_.value()).toDS()
        logger.debug("Taking message from sparkContext and parsing it")
        val data = sqlContext.read.schema(schema).json(dataSetRDD)
          .select(
            unix_timestamp($"timestamp", "yyyy-MM-dd HH:mm:ss").cast(TimestampType).as("timestamp"),
            $"status",
            $"message",
            from_unixtime(unix_timestamp($"timestamp"), "yyyy-MM-dd").cast(DateType).as("dt"),
            $"event_type")
            .filter(row => !row.anyNull)


        data.show() //TODO удалить после введения логирования
        data.write.insertInto("svpbigdata4.messages")
        offsetManager.saveOffset(rdd)
        val tableName = "svpbigdata4.messages"

        logger.info(s"Wrote message: ${data.col("message").toString()} into $tableName")

      } else {
        logger.info("Empty collection!")
      }
    }
    logger.debug("Writing message and its offset finished")

    logger.debug("Starting streaming context")
    ssc.start()
    logger.debug("Streaming context started")
    ssc.awaitTermination()
    logger.debug("Streaming context finished")
  }
}
