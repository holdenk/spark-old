package spark

import akka.actor.Actor
import akka.actor.Actor._
import akka.actor.ActorRef
import akka.dispatch.MessageDispatcher
import java.io._

sealed trait EventReporterMessage
case class ReportException(exception: Throwable, task: Task[_]) extends EventReporterMessage
case class ReportRDDChecksum(rddId: Int, splitIndex: Int, checksum: Int) extends EventReporterMessage
case class ReportRuntimeStatistics(
  rddId: Int,
  splitIndex: Int,
  mean: Double,
  stdDev: Double
) extends EventReporterMessage
case class ReportSerializationTime(time: Long) extends EventReporterMessage
case class StopEventReporter extends EventReporterMessage

class EventReporterActor(dispatcher: MessageDispatcher, eventLogWriter: EventLogWriter) extends Actor with Logging {
  self.dispatcher = dispatcher

  def receive = {
    case ReportException(exception, task) =>
      eventLogWriter.log(ExceptionEvent(exception, task))
    case ReportRDDChecksum(rddId, splitIndex, checksum) =>
      eventLogWriter.log(RDDChecksum(rddId, splitIndex, checksum))
    case ReportRuntimeStatistics(rddId, splitIndex, mean, stdDev) =>
      eventLogWriter.log(RuntimeStatistics(rddId, splitIndex, mean, stdDev))
    case ReportSerializationTime(time) =>
      eventLogWriter.log(SerializationTime(time))
    case StopEventReporter =>
      eventLogWriter.stop()
      self.reply('OK)
  }
}

class EventReporter(isMaster: Boolean, dispatcher: MessageDispatcher) extends Logging {
  val host = System.getProperty("spark.master.host")
  var eventLogWriter: Option[EventLogWriter] = if (isMaster) Some(new EventLogWriter) else None
  val measurePerformance =
    System.getProperty("spark.logging.measurePerformance", "false").toBoolean

  // Remote reference to the actor on workers
  var reporterActor: ActorRef = {
    for (elw <- eventLogWriter) {
      remote.register("EventReporter", actorOf(new EventReporterActor(dispatcher, elw)))
    }
    val port = System.getProperty("spark.master.akkaPort").toInt
    logInfo("Binding to Akka at %s:%d".format(host, port))
    remote.actorFor("EventReporter", host, port)
  }

  def reportException(exception: Throwable, task: Task[_]) {
    // TODO: The task may refer to an RDD, so sending it through the
    // actor will interfere with RDD back-referencing, causing a
    // duplicate version of the referenced RDD to be serialized. If
    // tasks had IDs, we could just send those, but they don't.
    reporterActor ! ReportException(exception, task)
  }

  def reportLocalException(exception: Throwable, task: Task[_]) {
    for (elw <- eventLogWriter)
      elw.log(ExceptionEvent(exception, task))
  }

  def reportRDDCreation(rdd: RDD[_], location: Array[StackTraceElement]) {
    // Bypass the actor for this to avoid serializing the RDD, which
    // would interfere with the automatic back-referencing done during
    // Java serialization.
    for (elw <- eventLogWriter)
      elw.log(RDDCreation(rdd, location))
  }

  def reportRDDChecksum(rdd: RDD[_], split: Split, checksum: Int) {
    reporterActor ! ReportRDDChecksum(rdd.id, split.index, checksum)
  }

  def reportRuntimeStatistics(rdd: RDD[_], split: Split, mean: Double, stdDev: Double) {
    reporterActor ! ReportRuntimeStatistics(rdd.id, split.index, mean, stdDev)
  }

  def reportSerialization(time: Long) {
    reporterActor ! ReportSerializationTime(time)
  }

  def stop() {
    reporterActor !! StopEventReporter
    eventLogWriter = None
    reporterActor = null
  }
}
