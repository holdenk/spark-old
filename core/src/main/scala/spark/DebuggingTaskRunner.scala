package spark

object DebuggingTaskRunner {
  def main(args: Array[String]) {
    if (args.length < 4) {
      System.err.println("Usage: DebuggingTaskRunner <eventLogPath> " +
                         "<taskStageId> <taskPartition> <host> " +
                         "<frameworkName> [<sparkHome> [<JAR path> ...]]")
      System.exit(1)
    }

    val eventLogPath = args(0)
    val taskStageId = args(1).toInt
    val taskPartition = args(2).toInt
    val host = args(3)
    val frameworkName = args(4)
    val sparkHome = if (args.length > 5) Option(args(5)) else None
    val jars = args.slice(6, args.length)

    val sc = new SparkContext(
      host, frameworkName, sparkHome match {
        case Some(x) => x; case None => null
      }, jars)

    val r = new EventLogReader(sc, Some(eventLogPath))

    r.taskWithId(taskStageId, taskPartition) match {
      case Some(task) =>
        task.run(0)
      case None =>
        System.err.println(
          "Task ID (%d, %d) is not associated with a task!".format(
            taskStageId, taskPartition))
    }
  }
}
