package org.training.spark.misc

import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.HashMap

class BasicRDDTest(sc: SparkContext) {

  def testBasicOperations(): Unit = {
    val nums = sc.makeRDD(Array(1, 2, 3, 4), 2)
    nums.collect()
    nums.reduce(_ + _)
    nums.fold(0)(_ + _)
    nums.map(_.toString).collect()
    nums.filter(_ > 2).collect()
    nums.flatMap(x => 1 to x).collect()
    nums.union(nums).collect()
    nums.glom().map(_.toList).collect()
    nums.collect({ case i if i >= 3 => i.toString }).collect()
    nums.keyBy(_.toString).collect()
    !nums.isEmpty()
    nums.max()
    nums.min()

    val dups = sc.makeRDD(Array(1, 1, 2, 2, 3, 3, 4, 4), 2)
    dups.distinct().count()
    dups.distinct().collect
    dups.distinct(2).collect

    val partitionSums = nums.mapPartitions(iter => Iterator(iter.sum))
    partitionSums.collect()
    val partitionSumsWithSplit = nums.mapPartitionsWithIndex {
      case (split, iter) => Iterator((split, iter.sum))
    }
    partitionSumsWithSplit.collect()

    val partitionSumsWithIndex = nums.mapPartitionsWithIndex {
      case (split, iter) => Iterator((split, iter.sum))
    }
    partitionSumsWithIndex.collect()
  }

  def testAggregate(): Unit = {
    val pairs =
      sc.makeRDD(Array(("a", 1), ("b", 2), ("a", 2), ("c", 5), ("a", 3)))
    type StringMap = HashMap[String, Int]
    val emptyMap = new StringMap {
      override def default(key: String): Int = 0
    }
    val mergeElement: (StringMap, (String, Int)) => StringMap =
      (map, pair) => {
        map(pair._1) += pair._2
        map
      }
    val mergeMaps: (StringMap, StringMap) => StringMap = (map1, map2) => {
      for ((key, value) <- map2) {
        map1(key) += value
      }
      map1
    }
    val result = pairs.aggregate(emptyMap)(mergeElement, mergeMaps)
    result.toSet
  }

  def testRepartition(): Unit = {
    val data = sc.parallelize(1 to 1000, 10)
    val repartitioned1 = data.repartition(2)
    repartitioned1.glom().collect()
  }

  def testCoalseced(): Unit = {
    val data = sc.parallelize(1 to 10, 10)
    val coalesced1 = data.coalesce(2)
    coalesced1.glom().collect()

    val coalesced2 = data.coalesce(3)
    coalesced2.glom().collect()

    val coalesced3 = data.coalesce(10)
    coalesced3.glom().collect()

    val coalesced4 = data.coalesce(20)
    coalesced4.glom().collect()

    val coalesced5 = data.coalesce(1, shuffle = true)
    coalesced5.glom().collect()

    val coalesced6 = data.coalesce(20, shuffle = true)
    coalesced6.glom().collect()
  }

  def testZippedRDD(): Unit = {
    val nums = sc.makeRDD(Array(1, 2, 3, 4), 2)
    val zipped = nums.zip(nums.map(_ + 1.0))
    zipped.glom().map(_.toList).collect()
  }

  def testTake(): Unit = {
    val nums = sc.makeRDD(Range(1, 1000), 1)
    nums.take(0)
    nums.take(3)
    nums.take(1001)
  }

  def testTop(): Unit = {
    val words = Vector("a", "b", "c", "d")
    implicit val ord = implicitly[Ordering[String]].reverse
    val rdd = sc.makeRDD(words, 2)
    rdd.top(2)

    val nums = Array(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
    val rdd2 = sc.makeRDD(nums, 2)
    rdd2.takeOrdered(5)
  }

  def testSortByKey(): Unit = {
    val data = sc.parallelize(Seq("5|50|A", "4|60|C", "6|40|B"))

    val col1 = Array("4|60|C", "5|50|A", "6|40|B")
    val col2 = Array("6|40|B", "5|50|A", "4|60|C")
    val col3 = Array("5|50|A", "6|40|B", "4|60|C")

    data.sortBy(_.split("\\|")(0)).collect()
    data.sortBy(_.split("\\|")(1)).collect()
    data.sortBy(_.split("\\|")(2)).collect()
  }

  def testSortByKeyWithOrdering(): Unit = {
    val data = sc.parallelize(
      Seq("Bob|Smith|50",
          "Jane|Smith|40",
          "Thomas|Williams|30",
          "Karen|Williams|60"))

    val ageOrdered = Array("Thomas|Williams|30",
                           "Jane|Smith|40",
                           "Bob|Smith|50",
                           "Karen|Williams|60")

    // last name, then first name
    val nameOrdered = Array("Bob|Smith|50",
                            "Jane|Smith|40",
                            "Karen|Williams|60",
                            "Thomas|Williams|30")

    val parse = (s: String) => {
      val split = s.split("\\|")
      Person(split(0), split(1), split(2).toInt)
    }

    import scala.reflect.classTag
    data.sortBy(parse, true, 2)(AgeOrdering, classTag[Person]).collect()
    data.sortBy(parse, true, 2)(NameOrdering, classTag[Person]).collect()
  }

  def testIntersection(): Unit = {
    val all = sc.parallelize(1 to 10)
    val evens = sc.parallelize(2 to 10 by 2)
    val intersection = Array(2, 4, 6, 8, 10)
    all.intersection(evens).collect()
    evens.intersection(all).collect()
  }

  def testZipWithIndex(): Unit = {
    val n = 10
    val data = sc.parallelize(0 until n, 3)
    val ranked = data.zipWithIndex()
    ranked.collect()
    val uniq = data.zipWithUniqueId()
    uniq.collect()
  }

}

case class Person(first: String, last: String, age: Int)

object AgeOrdering extends Ordering[Person] {
  def compare(a: Person, b: Person): Int = a.age.compare(b.age)
}

object NameOrdering extends Ordering[Person] {
  def compare(a: Person, b: Person): Int =
    implicitly[Ordering[Tuple2[String, String]]]
      .compare((a.last, a.first), (b.last, b.first))
}

object BasicRDDTest {

  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setAppName("Basic RDD Test")
      .setMaster("local[*]")
    val sc = new SparkContext(conf)

    val suite = new BasicRDDTest(sc)
    suite.testBasicOperations()
    sc.stop()
  }

}
