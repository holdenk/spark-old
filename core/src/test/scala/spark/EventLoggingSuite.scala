package spark

import java.io.File

import scala.io.Source

import com.google.common.io.Files
import org.scalatest.FunSuite
import org.apache.hadoop.io._

import SparkContext._

class EventLoggingSuite extends FunSuite {
  test("restore ParallelCollection from log") {
    // Initialize event log
    val tempDir = Files.createTempDir()
    val eventLog = new File(tempDir, "eventLog1")
    System.setProperty("spark.logging.eventLog", eventLog.getAbsolutePath)

    // Make an RDD
    val sc = new SparkContext("local", "test")
    val nums = sc.makeRDD(1 to 4)
    sc.stop()

    // Read it back from the event log
    val sc2 = new SparkContext("local", "test2")
    val r = new EventLogReader(sc2)
    assert(r.rdds.length === 1)
    assert(r.rdds(0).collect.toList === (1 to 4).toList)
    sc2.stop()

    System.clearProperty("spark.logging.eventLog")
  }

  test("interactive event log reading") {
    // Initialize event log
    val tempDir = Files.createTempDir()
    val eventLog = new File(tempDir, "eventLog2")
    System.setProperty("spark.logging.eventLog", eventLog.getAbsolutePath)

    // Make an RDD
    val sc = new SparkContext("local", "test")
    val env = SparkEnv.get
    val nums = sc.makeRDD(1 to 4)

    val sc2 = new SparkContext("local", "test2")
    val env2 = SparkEnv.get
    SparkEnv.set(env)

    // Read the RDD back from the event log
    val r = new EventLogReader(sc2)
    assert(r.rdds.length === 1)
    assert(r.rdds(0).collect.toList === List(1, 2, 3, 4))

    // Make another RDD
    val nums2 = sc.makeRDD(1 to 5)

    // Read it back from the event log
    r.loadNewEvents()
    assert(r.rdds.length === 2)
    assert(r.rdds(1).collect.toList === List(1, 2, 3, 4, 5))

    sc.stop()
    sc2.stop()
    System.clearProperty("spark.logging.eventLog")
  }

  test("set nextRddId after restoring") {
    // Initialize event log
    val tempDir = Files.createTempDir()
    val eventLog = new File(tempDir, "eventLog3")
    System.setProperty("spark.logging.eventLog", eventLog.getAbsolutePath)

    // Make some RDDs
    val sc = new SparkContext("local", "test")
    val env = SparkEnv.get
    val nums = sc.makeRDD(1 to 4)
    val numsMapped = nums.map(x => x + 1)
    sc.stop()

    val sc2 = new SparkContext("local", "test2")
    val env2 = SparkEnv.get

    // Read them back from the event log
    val r = new EventLogReader(sc2)
    assert(r.rdds.length === 2)

    // Make a new RDD and check for ID conflicts
    SparkEnv.set(env2)
    val nums2 = sc2.makeRDD(1 to 5)
    assert(nums2.id != r.rdds(0).id)
    assert(nums2.id != r.rdds(1).id)

    sc2.stop()
    System.clearProperty("spark.logging.eventLog")
  }
}
