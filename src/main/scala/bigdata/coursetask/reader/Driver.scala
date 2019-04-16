package bigdata.coursetask.reader

import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}

object Driver extends App {

  private val path = args(0).toString + "/"

  val sc = SparkSession
    .builder()
    .appName("AppName")
    .config("spark.master", "local[*]")
    .getOrCreate()

  sc.sparkContext.hadoopConfiguration.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")

  def createContext: StreamingContext = {
    val ssc = new StreamingContext(sc.sparkContext, batchDuration = Seconds(5))
    val inputStream = ssc.receiverStream(new FileReciever(path))
    val transformedStream = inputStream.transform(new Transformer)
    transformedStream.foreachRDD(rdd => if (!rdd.isEmpty()) new Writer()(rdd))

    ssc
  }

  sc.sparkContext.addJar("/libs/template.jar")

  val ssc = createContext

  ssc.start()
  ssc.awaitTermination()
}