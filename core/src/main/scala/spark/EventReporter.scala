package spark

import akka.actor.Actor
import akka.actor.Actor._
import akka.actor.ActorRef
import akka.dispatch.MessageDispatcher
import java.io._

sealed trait EventReporterMessage
case class ReportException(exception: Throwable) extends EventReporterMessage
case class ReportRDDCreation(rdd: RDD[_]) extends EventReporterMessage
case class ReportRDDChecksum(rdd: RDD[_], split: Split, checksum: Int) extends EventReporterMessage

class EventReporterActor(dispatcher: MessageDispatcher) extends Actor with Logging {
  self.dispatcher = dispatcher

  val eventLogWriter = new EventLogWriter

  def receive = {
    case ReportException(exception) =>
      eventLogWriter.log(ExceptionEvent(exception))
    case ReportRDDCreation(rdd) =>
      eventLogWriter.log(RDDCreation(rdd))
    case ReportRDDChecksum(rdd, split, checksum) =>
      eventLogWriter.log(RDDChecksum(rdd, split, checksum))
  }
}

class EventReporter(isMaster: Boolean) extends Logging {
  val host = System.getProperty("spark.master.host")
  val port = System.getProperty("spark.master.akkaPort").toInt

  // Remote reference to the actor on workers
  var reporterActor: ActorRef = {
    if (isMaster) {
      val dispatcher = new DaemonDispatcher("mydispatcher")
      remote.start(host, port).register("EventReporter", actorOf(new EventReporterActor(dispatcher)))
    }
    remote.actorFor("EventReporter", host, port)
  }

  def reportException(exception: Throwable) {
    reporterActor ! ReportException(exception)
  }

  def reportRDDCreation(rdd: RDD[_]) {
    reporterActor ! ReportRDDCreation(rdd)
  }

  def reportRDDChecksum(rdd: RDD[_], split: Split, checksum: Int) {
    reporterActor ! ReportRDDChecksum(rdd, split, checksum)
  }
}
