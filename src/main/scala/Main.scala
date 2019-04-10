import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.sql.types.{DataTypes, StructType, TimestampType}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.{Seconds, StreamingContext}


object Main {
  case class Message(message: String, status: String, eventType: String, timeStamp: TimestampType)

  def main(args: Array[String]): Unit = {

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "t510:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "use_a_separate_group_id_for_each_stream",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val sc = new SparkConf().setAppName("KafkaConsumer")
    val ssc: StreamingContext = new StreamingContext(sc, Seconds(2))
    ssc.checkpoint("checkpoint")

    val topics = Array("messages")
    val stream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )

    val struct = new StructType()
      .add("message", DataTypes.StringType)
      .add("status", DataTypes.StringType)
      .add("event_type", DataTypes.StringType)
      .add("timestamp", DataTypes.TimestampType)

    // где - то тут парсится json
    stream.foreachRDD { (rdd, time) =>
      //val data = rdd.map(record => record.value)
      //val json = spark.read//.schema(struct)//.json(data)
      //val result = json.groupBy($"action").agg(count("*").alias("count"))
      //result.show
    }

    // это часть осталась от простейшего консумера, который просто на экран выводит все, что приходит
    // как научимся json парсить, выпилим этот показательный примерчик
    val results: DStream[(String, String)] = stream.map(record => (record.key, record.value))
    val lines: DStream[String] = results.map(tuple => tuple._2)
    val words = lines.flatMap(x => x.split(""))
    lines.print()

    ssc.start()


  }

}
