package bigdata.coursetask.reader

import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DateType, StringType, StructType, TimestampType}
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
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

    val zkClient = getZkCurator()


    val inputDStream = KafkaUtils.createDirectStream(
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams, fromOffsets))


    val schema = new StructType()
      .add("message", StringType)
      .add("status", StringType)
      .add("event_type", StringType)
      .add("timestamp", TimestampType)


    // где - то тут парсится json
    stream.foreachRDD { rdd =>
      if (!rdd.isEmpty()) {
        //val offset = rdd.map(_.offset()).first()


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
      }
    }


    ssc.start()
    ssc.awaitTermination()
  }

  def getZkCurator() = {
    import org.apache.curator.retry.RetryNTimes
    val sleepMsBetweenRetries = 100
    val maxRetries = 3
    val retryPolicy = new RetryNTimes(maxRetries, sleepMsBetweenRetries)

    CuratorFrameworkFactory.newClient("t510:2181", retryPolicy)
  }
}
