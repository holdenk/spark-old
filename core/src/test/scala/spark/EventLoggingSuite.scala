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
}
