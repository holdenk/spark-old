package spark

import java.io._
import scala.collection.mutable.ArrayBuffer

sealed trait EventLogEntry
case class ExceptionEvent(exception: Throwable) extends EventLogEntry
case class RDDCreation(rdd: RDD[_], location: Array[StackTraceElement]) extends EventLogEntry
case class RDDChecksum(rddId: Int, splitIndex: Int, checksum: Int) extends EventLogEntry

class EventLogWriter(eventLogPath: Option[String] = None) extends Logging {
  val eventLog = for {
    elp <- eventLogPath orElse { Option(System.getProperty("spark.logging.eventLog")) }
    file = new File(elp)
    if !file.exists
  } yield new EventLogOutputStream(new FileOutputStream(file))

  def log(entry: EventLogEntry) {
    for (l <- eventLog) {
      l.writeObject(entry)
      l.flush()
    }
  }

  def stop() {
    for (l <- eventLog)
      l.close()
  }
}

class EventLogOutputStream(out: OutputStream) extends ObjectOutputStream(out)

class EventLogInputStream(in: InputStream, val sc: SparkContext) extends ObjectInputStream(in) {
  override def resolveClass(desc: ObjectStreamClass) =
    Class.forName(desc.getName, false, Thread.currentThread.getContextClassLoader)
}

class EventLogReader(sc: SparkContext, eventLogPath: Option[String] = None) {
  val objectInputStream = for {
    elp <- eventLogPath orElse { Option(System.getProperty("spark.logging.eventLog")) }
    file = new File(elp)
    if file.exists
  } yield new EventLogInputStream(new FileInputStream(file), sc)
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
    for (ois <- objectInputStream) {
      try {
        while (true) {
          val event = ois.readObject.asInstanceOf[EventLogEntry]
          events += event
          event match {
            case RDDCreation(rdd, location) => sc.updateRddId(rdd.id)
            case _ => {}
          }
        }
      } catch {
        case e: EOFException => {}
      }
    }
  }
}
