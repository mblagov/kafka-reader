package bigdata.coursetask.reader

import bigdata.coursetask.reader.Driver.sc
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SaveMode

class Writer extends (RDD[(String, Int)] => Unit) {
  override def apply(rdd: RDD[(String, Int)]) = {
    import sc.implicits._

    val result = rdd.toDF("Word", "Count")
    result.coalesce(1)
      .write
      .format("parquet")
      .mode(SaveMode.Append)
      .save("ssh://students@188.134.91.77:2534/store/data.parquet")
  }
}