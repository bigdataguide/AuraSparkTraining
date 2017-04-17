package org.training.spark.misc

import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

abstract class VectorAccumulator(val n: Int)
    extends AccumulatorV2[Int, Array[Int]] {

  protected val values = new Array[Int](n)

  override def isZero: Boolean = values.forall(_ == 0)

  override def merge(other: AccumulatorV2[Int, Array[Int]]): Unit = {
    other match {
      case o: VectorAccumulator =>
        require(this.n == o.n)
        (0 until n).foreach(i => {
          values(i) += o.values(i)
        })
      case _ =>
        throw new UnsupportedOperationException(
          s"Cannot merge ${this.getClass.getName} with ${other.getClass.getName}")
    }
  }

  override def copy(): AccumulatorV2[Int, Array[Int]] = {
    val clone = newInstance
    clone.merge(this)
    clone
  }

  override def value: Array[Int] = values

  override def reset(): Unit = {
    (0 until n).foreach(i => values(i) = 0)
  }

  protected def newInstance: VectorAccumulator
}

class LogHistAccumulator extends VectorAccumulator(5) {

  override protected def newInstance: VectorAccumulator =
    new LogHistAccumulator

  override def add(v: Int): Unit = {
    if (v < 5) {
      values(0) += 1
    } else if (v < 10) {
      values(1) += 1
    } else if (v < 20) {
      values(2) += 1
    } else if (v < 50) {
      values(3) += 1
    } else {
      values(4) += 1
    }
  }

  override def toString(): String = {
    s"""
       |[0,5) ${values(0)}
       |[5,10) ${values(1)}
       |[10, 20) ${values(2)}
       |[20, 50) ${values(3)}
       |[50, 100] ${values(4)}
       """.stripMargin
  }
}

object LogCounter {

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("Log Counter")
    val sc = new SparkContext(conf)

    val c1 = sc.longAccumulator("success")
    val c2 = sc.longAccumulator("failed")
    val hist = new LogHistAccumulator
    sc.register(hist, "hist")

    val input = if (args.length > 0) args(0) else "data/access.log"
    val lines = sc.textFile(input, 3)
    val logs = lines.flatMap(LogUtils.parseLine)

    logs.foreach {
      case (status, ms, url) => {
        if (status == 200) {
          c1.add(1)
          // println(s"${c1.value}")
        } else {
          c2.add(1)
        }
        hist.add(ms)
      }
    }

    println(s"success: ${c1.value}, failed: ${c2.value}")
    println(s"histogram ${hist.toString()}")

    sc.stop()
  }

}
