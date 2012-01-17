package spark

import scala.util.MurmurHash

class ChecksummingIterator[T](rdd: RDD[T], split: Split, underlying: Iterator[T]) extends Iterator[T] {
  // Use a constant seed so the checksum is reproducible
  val runningChecksum = new MurmurHash[T](0)

  // Intermediate values for computing statistics; see
  // http://www.johndcook.com/standard_deviation.html
  var n = 0
  var oldM = 0.0
  var newM = 0.0
  var oldS = 0.0
  var newS = 0.0

  var alreadySentReport = false

  val measurePerformance = SparkEnv.get.eventReporter.measurePerformance

  def hasNext = {
    val doesHaveNext = underlying.hasNext
    if (!doesHaveNext && !alreadySentReport) {
      reportChecksum()
      reportStatistics()
      alreadySentReport = true
    }
    doesHaveNext
  }

  def next(): T =
    if (measurePerformance) {
      val start = System.currentTimeMillis
      val result = underlying.next
      val end = System.currentTimeMillis
      updateChecksum(result)
      updateStatistics(end - start)
      result
    } else {
      val result = underlying.next
      updateChecksum(result)
      result
    }

  def updateChecksum(nextVal: T) {
    runningChecksum(nextVal)
    if (!underlying.hasNext && !alreadySentReport) {
      reportChecksum()
      reportStatistics()
      alreadySentReport = true
    }
  }

  def updateStatistics(nextTime: Double) {
    n += 1
    if (n == 1) {
      oldM = nextTime
      newM = nextTime
      oldS = 0.0
    } else {
      newM = oldM + (nextTime - oldM) / n
      newS = oldS + (nextTime - oldM) * (nextTime - newM)
      oldM = newM
      oldS = newS
    }
  }

  def reportChecksum() {
    SparkEnv.get.eventReporter.reportRDDChecksum(rdd, split, runningChecksum.hash)
  }

  def reportStatistics() {
    if (measurePerformance) {
      val mean = if (n > 0) newM else 0.0
      val stdDev = Math.sqrt(if (n > 1) newS / (n - 1) else 0.0)
      SparkEnv.get.eventReporter.reportRuntimeStatistics(rdd, split, mean, stdDev)
    }
  }
}
