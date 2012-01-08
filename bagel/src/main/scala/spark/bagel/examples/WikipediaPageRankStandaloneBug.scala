package spark.bagel.examples

import spark._
import spark.SparkContext._

import spark.bagel._
import spark.bagel.Bagel._

import scala.xml.{XML,NodeSeq}

import scala.collection.mutable.ArrayBuffer

import java.io.{InputStream, OutputStream, DataInputStream, DataOutputStream}

object WikipediaPageRankStandaloneBug {
  def main(args: Array[String]) {
    if (args.length < 5) {
      System.err.println("Usage: WikipediaPageRankStandaloneBug <inputFile> <threshold> <numIterations> <host> <usePartitioner>")
      System.exit(-1)
    }

    System.setProperty("spark.serializer", "spark.bagel.examples.WPRSerializer")

    val inputFile = args(0)
    val threshold = args(1).toDouble
    val numIterations = args(2).toInt
    val host = args(3)
    val usePartitioner = args(4).toBoolean
    val sc = new SparkContext(host, "WikipediaPageRankStandaloneBug")

    val input = sc.textFile(inputFile)
    val partitioner = new HashPartitioner(sc.defaultParallelism)
    val links =
      if (usePartitioner)
        input.map(parseArticle _).partitionBy(partitioner).cache
      else
        input.map(parseArticle _).cache
    val n = links.count
    val defaultRank = 1.0 / n
    val a = 0.15

    // Do the computation
    val startTime = System.currentTimeMillis
    val ranks =
        pageRank(links, numIterations, defaultRank, a, n, partitioner, usePartitioner, sc.defaultParallelism)

    // Print the result
    System.err.println("Articles with PageRank >= "+threshold+":")
    val top =
      (ranks
       .filter { case (id, rank) => rank >= threshold }
       .map { case (id, rank) => "%s\t%s\n".format(id, rank) }
       .collect.mkString)
    println(top)

    val time = (System.currentTimeMillis - startTime) / 1000.0
    println("Completed %d iterations in %f seconds: %f seconds per iteration"
            .format(numIterations, time, time / numIterations))
    System.exit(0)
  }

  def parseArticle(line: String): (String, Array[String]) = {
    val fields = line.split("\t")
    val (title, body) = (fields(1), fields(3).replace("\\n", "\n"))
    val id = new String(title)
    val links = XML.loadString(body) \\ "link" \ "target"
    val outEdges = links.map(link => new String(link.text)).toArray
    (id, outEdges)
  }

  def pageRank(
    links: RDD[(String, Array[String])],
    numIterations: Int,
    defaultRank: Double,
    a: Double,
    n: Long,
    partitioner: Partitioner,
    usePartitioner: Boolean,
    numSplits: Int
  ): RDD[(String, Double)] = {
    var ranks = links.mapValues { edges => defaultRank }
    for (i <- 1 to numIterations) {
      val contribs = links.groupWith(ranks).flatMap {
        case (id, (linksWrapper, rankWrapper)) =>
          if (linksWrapper.length > 0) {
            if (rankWrapper.length > 0) {
              linksWrapper(0).map(dest => (dest, rankWrapper(0) / linksWrapper(0).size))
            } else {
              linksWrapper(0).map(dest => (dest, defaultRank / linksWrapper(0).size))
            }
          } else {
            Array[(String, Double)]()
          }
      }
      ranks = (contribs.combineByKey((x: Double) => x,
                                     (x: Double, y: Double) => x + y,
                                     (x: Double, y: Double) => x + y,
                                     numSplits,
                                     partitioner)
               .mapValues(sum => a/n + (1-a)*sum))
    }
    ranks
  }
}
