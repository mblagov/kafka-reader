package bigdata.coursetask.reader.models

import org.apache.spark.sql.types.TimestampType

case class Message(message: String, status: String, eventType: String, timeStamp: TimestampType)
