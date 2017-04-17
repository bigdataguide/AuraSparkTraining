package org.training.spark.misc

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.HashSet

class PairRDDTest(sc: SparkContext) {

  test("aggregateByKey") {
    val pairs =
      sc.parallelize(Array((1, 1), (1, 1), (3, 2), (5, 1), (5, 3)), 2)
    val sets =
      pairs.aggregateByKey(new HashSet[Int]())(_ += _, _ ++= _).collect()
    val valuesFor1 = sets.find(_._1 == 1).get._2
    val valuesFor3 = sets.find(_._1 == 3).get._2
    val valuesFor5 = sets.find(_._1 == 5).get._2
  }

  test("groupByKey") {
    val pairs = sc.parallelize(Array((1, 1), (1, 2), (1, 3), (2, 1)))
    val groups = pairs.groupByKey().collect()
    val valuesFor1 = groups.find(_._1 == 1).get._2
    val valuesFor2 = groups.find(_._1 == 2).get._2
  }

  test("groupByKey with duplicates") {
    val pairs = sc.parallelize(Array((1, 1), (1, 2), (1, 3), (1, 1), (2, 1)))
    val groups = pairs.groupByKey().collect()
    val valuesFor1 = groups.find(_._1 == 1).get._2
    val valuesFor2 = groups.find(_._1 == 2).get._2
  }

  test("reduceByKey") {
    val pairs = sc.parallelize(Array((1, 1), (1, 2), (1, 3), (1, 1), (2, 1)))
    val sums = pairs.reduceByKey(_ + _).collect()
  }

  test("reduceByKey with collectAsMap") {
    val pairs = sc.parallelize(Array((1, 1), (1, 2), (1, 3), (1, 1), (2, 1)))
    val sums = pairs.reduceByKey(_ + _).collectAsMap()
  }

  test("join") {
    val rdd1 = sc.parallelize(Array((1, 1), (1, 2), (2, 1), (3, 1)))
    val rdd2 = sc.parallelize(Array((1, 'x'), (2, 'y'), (2, 'z'), (4, 'w')))
    val joined = rdd1.join(rdd2).collect()
  }

  test("join all-to-all") {
    val rdd1 = sc.parallelize(Array((1, 1), (1, 2), (1, 3)))
    val rdd2 = sc.parallelize(Array((1, 'x'), (1, 'y')))
    val joined = rdd1.join(rdd2).collect()
  }

  test("leftOuterJoin") {
    val rdd1 = sc.parallelize(Array((1, 1), (1, 2), (2, 1), (3, 1)))
    val rdd2 = sc.parallelize(Array((1, 'x'), (2, 'y'), (2, 'z'), (4, 'w')))
    val joined = rdd1.leftOuterJoin(rdd2).collect()
  }

  test("rightOuterJoin") {
    val rdd1 = sc.parallelize(Array((1, 1), (1, 2), (2, 1), (3, 1)))
    val rdd2 = sc.parallelize(Array((1, 'x'), (2, 'y'), (2, 'z'), (4, 'w')))
    val joined = rdd1.rightOuterJoin(rdd2).collect()
  }

  test("fullOuterJoin") {
    val rdd1 = sc.parallelize(Array((1, 1), (1, 2), (2, 1), (3, 1)))
    val rdd2 = sc.parallelize(Array((1, 'x'), (2, 'y'), (2, 'z'), (4, 'w')))
    val joined = rdd1.fullOuterJoin(rdd2).collect()
  }

  test("groupWith") {
    val rdd1 = sc.parallelize(Array((1, 1), (1, 2), (2, 1), (3, 1)))
    val rdd2 = sc.parallelize(Array((1, 'x'), (2, 'y'), (2, 'z'), (4, 'w')))
    val joined = rdd1.groupWith(rdd2).collect()
    val joinedSet =
      joined.map(x => (x._1, (x._2._1.toList, x._2._2.toList))).toSet
  }

  test("groupWith3") {
    val rdd1 = sc.parallelize(Array((1, 1), (1, 2), (2, 1), (3, 1)))
    val rdd2 = sc.parallelize(Array((1, 'x'), (2, 'y'), (2, 'z'), (4, 'w')))
    val rdd3 = sc.parallelize(Array((1, 'a'), (3, 'b'), (4, 'c'), (4, 'd')))
    val joined = rdd1.groupWith(rdd2, rdd3).collect()
    val joinedSet = joined
      .map(x => (x._1, (x._2._1.toList, x._2._2.toList, x._2._3.toList)))
      .toSet
  }

  test("keys and values") {
    val rdd = sc.parallelize(Array((1, "a"), (2, "b")))
    rdd.keys.collect()
    rdd.values.collect()
  }

  test("foldByKey") {
    val pairs = sc.parallelize(Array((1, 1), (1, 2), (1, 3), (1, 1), (2, 1)))
    val sums = pairs.foldByKey(0)(_ + _).collect()
  }

  test("lookup") {
    val pairs = sc.parallelize(Array((1, 2), (3, 4), (5, 6), (5, 7)))
    pairs.lookup(1)
    pairs.lookup(-1)
  }

  def test(msg: String)(body: => Unit): Unit = {
    println(s"test suite: $msg")
    body
  }
}

object PairRDDTest {
  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setAppName("PairRDDTest")
      .setMaster("local[*]")
    val sc = new SparkContext(conf)
    new PairRDDTest(sc)
    sc.stop()
  }
}
