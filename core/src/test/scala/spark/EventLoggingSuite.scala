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
    val eventLog = new File(tempDir, "eventLog").getAbsolutePath
    System.setProperty("spark.logging.eventLog", eventLog)

    // Make an RDD
    val sc = new SparkContext("local", "test")
    val nums = sc.makeRDD(1 to 4)

    // Read it back from the event log
    val sc2 = new SparkContext("local", "test2")
    val r = new EventLogReader(sc2)
    assert(r.rdds.length === 1)
    assert(r.rdds(0).collect.toList === nums.collect.toList)

    System.clearProperty("spark.logging.eventLog")
    sc.stop()
    sc2.stop()
  }

  test("interactive event log reading") {
    // Initialize event log
    val tempDir = Files.createTempDir()
    val eventLog = new File(tempDir, "eventLog").getAbsolutePath
    System.setProperty("spark.logging.eventLog", eventLog)

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

    System.clearProperty("spark.logging.eventLog")
    sc.stop()
    sc2.stop()
  }
}
