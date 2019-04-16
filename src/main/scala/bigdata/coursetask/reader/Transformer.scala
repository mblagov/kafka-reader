package bigdata.coursetask.reader

import org.apache.spark.rdd.RDD

class Transformer extends (RDD[String] => RDD[(String, Int)]) {
  override def apply(rdd: RDD[String]) = {
    rdd.flatMap(rec => rec.split(" "))
      .map(word => (word, 1))
      .reduceByKey((one, two) => one + two)
  }
}