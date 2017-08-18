
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

    val filePath = if (args.length > 0)  args(0) else "data/textfile/Hamlet.txt"
    val stopWordFile = if (args.length > 1) args(1) else "data/textfile/stopword.txt"

    val conf = new SparkConf().setAppName("Hamlet Word Count")
    val sc = new SparkContext(conf)

    val stopwords = sc.textFile(stopWordFile)
      .map(line => line.toLowerCase.trim)
      .collect().toSet

    val result = sc.textFile(filePath)
      .flatMap(line => splitWords(line))
      .filter(w => !stopwords.contains(w))
      .map((_, 1))
      .reduceByKey(_ + _)
      .sortBy(_._2, false)

    result.take(100).foreach(println)
  }

}
