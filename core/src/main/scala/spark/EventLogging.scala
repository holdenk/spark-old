package spark

import java.io._
import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer

sealed trait EventLogEntry
case class ExceptionEvent(exception: Throwable, task: Task[_]) extends EventLogEntry
case class RDDCreation(rdd: RDD[_], location: Array[StackTraceElement]) extends EventLogEntry
case class RDDChecksum(rddId: Int, splitIndex: Int, checksum: Int) extends EventLogEntry
case class RuntimeStatistics(
  rddId: Int,
  splitIndex: Int,
  mean: Double,
  stdDev: Double
) extends EventLogEntry
case class SerializationTime(time: Long) extends EventLogEntry

class EventLogWriter extends Logging {
  private var eventLog: Option[EventLogOutputStream] = None
  setEventLogPath(Option(System.getProperty("spark.logging.eventLog")))
  private var eventLogReader: Option[EventLogReader] = None

  def enableChecksumVerification(eventLogReader: EventLogReader) {
    this.eventLogReader = Some(eventLogReader)
  }

  def setEventLogPath(eventLogPath: Option[String]) {
    eventLog =
      for {
        elp <- eventLogPath
        file = new File(elp)
        if !file.exists
      } yield new EventLogOutputStream(new FileOutputStream(file))
  }

  def log(entry: EventLogEntry) {
    // Log the entry
    for (l <- eventLog) {
      l.writeObject(entry)
      l.flush()
    }
    // Do checksum verification if enabled
    for {
      r <- eventLogReader
      RDDChecksum(rddId, splitIndex, checksum) <- Some(entry)
      recordedChecksum <- r.checksumFor(rddId, splitIndex)
      if checksum != recordedChecksum.checksum
    } r.reportChecksumMismatch(recordedChecksum, RDDChecksum(rddId, splitIndex, checksum))
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

  // Enable checksum verification of loaded RDDs as they are computed
  for (w <- sc.env.eventReporter.eventLogWriter)
    w.enableChecksumVerification(this)

  val checksumMismatches = new ArrayBuffer[(RDDChecksum, RDDChecksum)]

  def rdds = for (RDDCreation(rdd, location) <- events) yield rdd

  def serializationTime = (for (SerializationTime(time) <- events) yield time).sum

  def printRDDs() {
    for (RDDCreation(rdd, location) <- events) {
      println("#%02d: %-20s %s".format(rdd.id, rddType(rdd), firstExternalElement(location)))
    }
  }

  def printProcessingTime() {
    def mean(xs: Seq[Double]) = xs.sum / xs.length
    println("RDD\tAverage processing time per element (ms)")
    println("---\t----------------------------------------")
    (events.collect { case rs: RuntimeStatistics => rs }
     .groupBy(_.rddId)
     .mapValues(xs => mean(xs.map(_.mean)))
     .toList.sortBy(_._2).reverse
     .foreach(x => println("#" + x._1 + "\t" + x._2)))
  }

  private def rddType(rdd: RDD[_]): String =
    rdd.getClass.getName.replaceFirst("""^spark\.""", "")

  def visualizeRDDs() {
    val file = File.createTempFile("spark-rdds-", "")
    val dot = new java.io.PrintWriter(file)
    dot.println("digraph {")
    for (RDDCreation(rdd, location) <- events) {
      dot.println("  %d [label=\"%d %s\"]".format(rdd.id, rdd.id, rddType(rdd)))
      for (dep <- rdd.dependencies)
        dot.println("  %d -> %d;".format(rdd.id, dep.rdd.id))
    }
    dot.println("}")
    dot.close()
    Runtime.getRuntime.exec("dot -Grankdir=BT -Tpdf " + file + " -o " + file + ".pdf")
    println(file + ".pdf")
  }

  def debugException(event: ExceptionEvent, debugOpts: String = "-Xdebug -agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=8000") {
    for {elp <- eventLogPath orElse { Option(System.getProperty("spark.logging.eventLog")) }
         sparkHome <- Option(sc.sparkHome) orElse { Option("") }}
      try {
        println("Running task " + event.task)
        val eventIndex = events.indexOf(event).toString

        // Launch the task in a separate JVM with debug options set
        val pb = new ProcessBuilder(List(
          "./run", "spark.DebuggingTaskRunner", elp, eventIndex, sc.master,
          sc.frameworkName, sparkHome) ::: sc.jars.toList)
        val env = pb.environment
        env.put("SPARK_DEBUG_OPTS", debugOpts)
        pb.redirectErrorStream(true)
        val proc = pb.start()

        // Pipe the task's stdout and stderr to our own
        new Thread {
          override def run {
            var byte: Int = -1
            val procStdout = proc.getInputStream
            byte = procStdout.read()
            while (byte != -1) {
              System.out.write(byte)
              byte = procStdout.read()
            }
          }
        }.start()
        proc.waitFor()
        println("Finished running task " + event.task)
    } catch {
      case ex =>
        println("Failed to run task %s: %s".format(event.task, ex))
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

  def checksumFor(rddId: Int, splitIndex: Int): Option[RDDChecksum] = events.collectFirst {
    case c: RDDChecksum if c.rddId == rddId && c.splitIndex == splitIndex => c
  }

  def reportChecksumMismatch(recordedChecksum: RDDChecksum, newChecksum: RDDChecksum) {
    checksumMismatches.append((recordedChecksum, newChecksum))
  }
}
