package spark

import java.io._
import scala.collection.mutable.ArrayBuffer

sealed trait EventLogEntry
case class ExceptionEvent(exception: Throwable) extends EventLogEntry
case class RDDCreation(rdd: RDD[_], location: Array[StackTraceElement]) extends EventLogEntry
case class RDDChecksum(rddId: Int, splitIndex: Int, checksum: Int) extends EventLogEntry

class EventLogWriter extends Logging {
  val eventLog =
    if (System.getProperty("spark.logging.eventLog") != null) {
      try {
        val file = new File(System.getProperty("spark.logging.eventLog"))
        if (!file.exists) {
          Some(new EventLogOutputStream(new FileOutputStream(file)))
        } else {
          logWarning("Event log %s already exists".format(System.getProperty("spark.logging.eventLog")))
          None
        }
      } catch {
        case e: FileNotFoundException =>
          logWarning("Can't write to %s: %s".format(System.getProperty("spark.logging.eventLog"), e))
          None
      }
    } else {
      None
    }

  def log(entry: EventLogEntry) {
    for (l <- eventLog) {
      l.writeObject(entry)
      l.flush()
    }
  }
}

class EventLogOutputStream(out: OutputStream) extends ObjectOutputStream(out)

class EventLogInputStream(in: InputStream, val sc: SparkContext) extends ObjectInputStream(in)

class EventLogReader(sc: SparkContext) {
  val ois = new EventLogInputStream(new FileInputStream(System.getProperty("spark.logging.eventLog")), sc)
  val events = new ArrayBuffer[EventLogEntry]
  loadNewEvents()

  def rdds = for (RDDCreation(rdd, location) <- events) yield rdd

  def printRDDs() {
    for (RDDCreation(rdd, location) <- events) {
      println("#%02d: %-20s %s".format(rdd.id, rdd.getClass.getName.replaceFirst("""^spark\.""", ""), firstExternalElement(location)))
    }
  }

  def firstExternalElement(location: Array[StackTraceElement]) =
    (location.tail.find(!_.getClassName.matches("""spark\.[A-Z].*"""))
      orElse { location.headOption }
      getOrElse { "" })

  def loadNewEvents() {
    try {
      while (true) {
        events += ois.readObject.asInstanceOf[EventLogEntry]
      }
    } catch {
      case e: EOFException => {}
    }
  }
}
