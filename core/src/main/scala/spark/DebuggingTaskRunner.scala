package spark

object DebuggingTaskRunner {
  def main(args: Array[String]) {
    if (args.length < 4) {
      System.err.println("Usage: DebuggingTaskRunner <eventLogPath> " +
                         "<eventIndex> <host> <frameworkName> [<sparkHome> " +
                         "[<JAR path> ...]]")
      System.exit(1)
    }

    val eventLogPath = args(0)
    val eventIndex = args(1).toInt
    val host = args(2)
    val frameworkName = args(3)
    val sparkHome = if (args.length > 4) Option(args(4)) else None
    val jars = args.slice(5, args.length)

    val sc = new SparkContext(
      host, frameworkName, sparkHome match {
        case Some(x) => x; case None => null
      }, jars)
    val r = new EventLogReader(sc, Some(eventLogPath))

    r.events(eventIndex) match {
      case ExceptionEvent(exception, task) =>
        task.run(0)
      case _ =>
        System.err.println("Event index " + eventIndex + " is not associated with a task!")
    }
  }
}
