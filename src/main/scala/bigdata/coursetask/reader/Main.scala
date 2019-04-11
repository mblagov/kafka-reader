package bigdata.coursetask.reader

import bigdata.coursetask.reader.models.Message
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{LongType, StringType, StructType}
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}

object Main {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .appName("KafkaConsumer")
      .getOrCreate()

    import spark.implicits._

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

    val schema = (new StructType)
      .add("payload", (new StructType)
        .add("event", (new StructType)
          .add("action", StringType)
          .add("timestamp", LongType)
        )
      )

    // где - то тут парсится json
    stream.foreachRDD { rdd =>
      val dataSetRDD = rdd.map(_.value()).toDS()
      val data = sqlContext.read.schema(schema).json(dataSetRDD).select($"action", $"timestamp".cast(LongType)).show
    }

    // это часть осталась от простейшего консумера, который просто на экран выводит все, что приходит
    // как научимся json парсить, выпилим этот показательный примерчик
    /*val results: DStream[(String, String)] = stream.map(record => (record.key, record.value))
    val lines: DStream[String] = results.map(tuple => tuple._2)
    val words = lines.flatMap(x => x.split(""))
    lines.print()*/

    ssc.start()
    ssc.awaitTermination()
  }

}
