
package org.training.spark.misc

import org.apache.spark.{SparkContext, SparkConf}

object HamletWordCount {

  def splitWords(line: String): Seq[String] = {
    line.replaceAll("['.,:?!-]", "")
      .split("\\s")
      .map(_.trim.toLowerCase)
      .filter(_.nonEmpty)
  }

  def main(args: Array[String]) {

    val filePath = if (args.length > 0) args(0) else "data/textfile/Hamlet.txt"
    val stopWordFile = if (args.length > 1) args(1) else "data/textfile/stopword.txt"

    val conf = new SparkConf()
      .setAppName("Hamlet Word Count")
      .setIfMissing("spark.master", "local")
    val sc = new SparkContext(conf)

    val lineCounts = sc.longAccumulator("total lines")
    val totalCounts = sc.longAccumulator("total words")
    val stopCounts = sc.longAccumulator("stop words")

    val stopwords = sc.textFile(stopWordFile)
      .map(line => line.toLowerCase.trim)
      .collect().toSet

    val broadcastStopwords = sc.broadcast(stopwords)

    val result = sc.textFile(filePath)
      .map { line =>
        lineCounts.add(1)
        line
      }
      .flatMap(line => splitWords(line))
      .filter { w =>
        val isStopWord = broadcastStopwords.value.contains(w)
        if (isStopWord) {
          stopCounts.add(1)
        }
        totalCounts.add(1)
        !isStopWord
      }
      .map((_, 1))
      .reduceByKey(_ + _)
      .sortBy(_._2, false)

    result.take(100).foreach(println)
    println("total lines:" + lineCounts.value + ", total words:" + totalCounts.value + ", stopWords:" + stopCounts.value)
  }

}
