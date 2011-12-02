package spark

import scala.util.MurmurHash

class ChecksummingIterator[T](rdd: RDD[T], split: Split, underlying: Iterator[T]) extends Iterator[T] {
  // Use a constant seed so the checksum is reproducible
  val runningChecksum = new MurmurHash[T](0)
  var alreadyReportedChecksum = false

  def hasNext = {
    val result = underlying.hasNext
    reportChecksum(result)
    result
  }

  def next(): T = {
    val result = underlying.next
    updateChecksum(result)
    result
  }

  def updateChecksum(nextVal: T) {
    runningChecksum(nextVal)
    reportChecksum(underlying.hasNext)
  }

  def reportChecksum(hasNext: Boolean) {
    if (!hasNext && !alreadyReportedChecksum) {
      SparkEnv.get.eventReporter.reportRDDChecksum(rdd, split, runningChecksum.hash)
      alreadyReportedChecksum = true
    }
  }
}
