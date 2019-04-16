package bigdata.coursetask.reader

import java.nio.file._
import java.util.concurrent.TimeUnit
import scala.collection.JavaConverters._

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver

import scala.io.Source

class FileReciever(location: String) extends Receiver[String](StorageLevel.MEMORY_ONLY) {

  override def onStart() = {
    val watcher = FileSystems.getDefault.newWatchService
    Paths.get(location).register(watcher, StandardWatchEventKinds.ENTRY_CREATE)

    println("Starting the reciever...")

    while (true) {
      watcher.poll(5, TimeUnit.SECONDS) match {
        case null =>;
        case events if events != null => {
          println("New events recieved...")

          for (event <- events.pollEvents.asScala) {
            val file = event.context().asInstanceOf[Path].getFileName.toString
            val lines = Source.fromFile(location + file).getLines()
            lines.foreach(store)
            Files.delete(Paths.get(location + file))
          }
          events.reset()
        }
      }
    }
  }

  override def onStop(): Unit = {
    println("Stopping the reciever...")
  }
}

