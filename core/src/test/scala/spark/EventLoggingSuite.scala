package spark

import java.io.File

import scala.io.Source

import com.google.common.io.Files
import org.scalatest.FunSuite
import org.apache.hadoop.io._

import SparkContext._

class EventLoggingSuite extends FunSuite {
  System.clearProperty("spark.logging.eventLog")

  test("restore ParallelCollection from log") {
    // Initialize event log
    val tempDir = Files.createTempDir()
    val eventLog = new File(tempDir, "eventLog")

    // Make an RDD
    val sc = new SparkContext("local", "test")
    for (w <- SparkEnv.get.eventReporter.eventLogWriter)
      w.setEventLogPath(Some(eventLog.getAbsolutePath))
    val nums = sc.makeRDD(1 to 4)
    sc.stop()

    // Read it back from the event log
    val sc2 = new SparkContext("local", "test2")
    val r = new EventLogReader(sc2, Some(eventLog.getAbsolutePath))
    assert(r.rdds.length === 1)
    assert(r.rdds(0).collect.toList === (1 to 4).toList)
    sc2.stop()
  }

  test("interactive event log reading") {
    // Initialize event log
    val tempDir = Files.createTempDir()
    val eventLog = new File(tempDir, "eventLog")

    // Make an RDD
    val sc = new SparkContext("local", "test")
    for (w <- SparkEnv.get.eventReporter.eventLogWriter)
      w.setEventLogPath(Some(eventLog.getAbsolutePath))
    val nums = sc.makeRDD(1 to 4)

    // Read the RDD back from the event log
    val sc2 = new SparkContext("local", "test2")
    val r = new EventLogReader(sc2, Some(eventLog.getAbsolutePath))
    assert(r.rdds.length === 1)
    assert(r.rdds(0).collect.toList === List(1, 2, 3, 4))

    // Make another RDD
    SparkEnv.set(sc.env)
    val nums2 = sc.makeRDD(1 to 5)

    // Read it back from the event log
    SparkEnv.set(sc2.env)
    r.loadNewEvents()
    assert(r.rdds.length === 2)
    assert(r.rdds(1).collect.toList === List(1, 2, 3, 4, 5))

    sc.stop()
    sc2.stop()
  }

  test("set nextRddId after restoring") {
    // Initialize event log
    val tempDir = Files.createTempDir()
    val eventLog = new File(tempDir, "eventLog")

    // Make some RDDs
    val sc = new SparkContext("local", "test")
    for (w <- SparkEnv.get.eventReporter.eventLogWriter)
      w.setEventLogPath(Some(eventLog.getAbsolutePath))
    val nums = sc.makeRDD(1 to 4)
    val numsMapped = nums.map(x => x + 1)
    sc.stop()

    // Read them back from the event log
    val sc2 = new SparkContext("local", "test2")
    val r = new EventLogReader(sc2, Some(eventLog.getAbsolutePath))
    assert(r.rdds.length === 2)

    // Make a new RDD and check for ID conflicts
    val nums2 = sc2.makeRDD(1 to 5)
    assert(nums2.id != r.rdds(0).id)
    assert(nums2.id != r.rdds(1).id)
    sc2.stop()
  }

  test("checksum verification") {
    // Initialize event log
    val tempDir = Files.createTempDir()
    val eventLog = new File(tempDir, "eventLog")

    // Make some RDDs that use Math.random
    val sc = new SparkContext("local", "test")
    for (w <- SparkEnv.get.eventReporter.eventLogWriter)
      w.setEventLogPath(Some(eventLog.getAbsolutePath))
    val nums = sc.makeRDD(1 to 4)
    val numsNondeterministic = nums.map(x => Math.random)
    val collected = numsNondeterministic.collect
    sc.stop()

    // Read them back from the event log
    val sc2 = new SparkContext("local", "test2")
    val r = new EventLogReader(sc2, Some(eventLog.getAbsolutePath))
    assert(r.rdds.length === 2)
    assert(r.rdds(0).collect.toList === (1 to 4).toList)
    assert(r.rdds(1).collect.toList != collected.toList)
    // Ensure that all checksums have gone through
    sc2.stop()

    // Make sure we found a checksum mismatch
    assert(r.checksumMismatches.find {
      case (RDDChecksum(rddId, _, _), _) if rddId == numsNondeterministic.id => true
      case _ => false
    }.nonEmpty)
  }

  test("runtime statistics logging") {
    System.setProperty("spark.logging.measurePerformance", "true")

    // Initialize event log
    val tempDir = Files.createTempDir()
    val eventLog = new File(tempDir, "eventLog")

    // Make an RDD that takes a long time to compute each element
    val sc = new SparkContext("local", "test")
    for (w <- SparkEnv.get.eventReporter.eventLogWriter)
      w.setEventLogPath(Some(eventLog.getAbsolutePath))
    val nums = sc.makeRDD(List(1, 2, 3))
    val slow = nums.map { x => Thread.sleep(1000); x }
    val slowList = slow.collect.toList
    sc.stop()

    // Verify that the runtime statistics make sense
    val sc2 = new SparkContext("local", "test2")
    val r = new EventLogReader(sc2, Some(eventLog.getAbsolutePath))
    val stats = r.events.collect {
      case RuntimeStatistics(rddId, _, mean, _) if rddId == slow.id =>
        mean
    }.toList
    assert(stats.length == 1)
    assert(stats.head >= 1000)
    sc2.stop()

    System.clearProperty("spark.logging.measurePerformance")
  }

  test("task submission logging") {
    // Initialize event log
    val tempDir = Files.createTempDir()
    val eventLog = new File(tempDir, "eventLog")

    // Make an RDD and do some computation to run tasks
    val sc = new SparkContext("local", "test")
    for (w <- SparkEnv.get.eventReporter.eventLogWriter)
      w.setEventLogPath(Some(eventLog.getAbsolutePath))
    val nums = sc.makeRDD(List(1, 2, 3))
    val nums2 = nums.map(_ * 2)
    val nums2List = nums2.collect.toList
    sc.stop()

    // Verify that tasks were logged
    val sc2 = new SparkContext("local", "test2")
    val r = new EventLogReader(sc2, Some(eventLog.getAbsolutePath))
    val tasks = r.events.collect { case t: TaskSubmission => t }
    assert(tasks.nonEmpty)
    sc2.stop()
  }
}
