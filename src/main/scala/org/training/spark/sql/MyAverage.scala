
package org.training.spark.sql

import org.apache.spark.sql.{SparkSession, Encoders, Encoder}
import org.apache.spark.sql.expressions.Aggregator

case class Employee(name: String, salary: Long)

case class Average(var sum: Long, var count: Long)

class MyAverage extends Aggregator[Employee, Average, Double] {

  def zero: Average = Average(0L, 0L)

  def reduce(buffer: Average, employee: Employee): Average = {
    buffer.sum += employee.salary
    buffer.count += 1
    buffer
  }

  def merge(b1: Average, b2: Average): Average = {
    b1.sum += b2.sum
    b1.count += b2.count
    b1
  }

  def finish(reduction: Average): Double = reduction.sum.toDouble / reduction.count

  def bufferEncoder: Encoder[Average] = Encoders.product

  def outputEncoder: Encoder[Double] = Encoders.scalaDouble
}

object MyAverage {

  def main(args: Array[String]) {
    val spark = SparkSession
      .builder()
      .appName("MyAverage")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val ds =
      """
        |{"name":"Michael", "salary":3000}
        |{"name":"Andy", "salary":4500}
        |{"name":"Justin", "salary":3500}
        |{"name":"Berta", "salary":4000}
      """.stripMargin.split("\n").toSeq.toDF().as[Employee]
    ds.show()

    val averageSalary = new MyAverage().toColumn.name("average_salary")
    val result = ds.select(averageSalary)
    result.show()
  }
}