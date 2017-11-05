package org.training.spark.sql

import org.apache.spark.sql.{SparkSession, Row}
import org.apache.spark.sql.expressions.{
  MutableAggregationBuffer,
  UserDefinedAggregateFunction
}
import org.apache.spark.sql.types._

class MyAverageUDAF extends UserDefinedAggregateFunction {
  def inputSchema: StructType =
    StructType(StructField("inputColumn", LongType) :: Nil)

  def bufferSchema: StructType = {
    StructType(
      StructField("sum", LongType) :: StructField("count", LongType) :: Nil)
  }

  def dataType: DataType = DoubleType

  def deterministic: Boolean = true

  def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = 0L
    buffer(1) = 0L
  }

  def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    if (!input.isNullAt(0)) {
      buffer(0) = buffer.getLong(0) + input.getLong(0)
      buffer(1) = buffer.getLong(1) + 1
    }
  }

  def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getLong(0) + buffer2.getLong(0)
    buffer1(1) = buffer1.getLong(1) + buffer2.getLong(1)
  }

  def evaluate(buffer: Row): Double =
    buffer.getLong(0).toDouble / buffer.getLong(1)
}

object MyAverageUDAF {

  def main(args: Array[String]) {
    val spark = SparkSession
      .builder()
      .appName("MyAverageUDAF")
      .master("local[*]")
      .getOrCreate()
    spark.udf.register("myAverage", new MyAverageUDAF())

    val rdd = spark.sparkContext.makeRDD("""
                                           |{"name":"Michael", "salary":3000}
                                           |{"name":"Andy", "salary":4500}
                                           |{"name":"Justin", "salary":3500}
                                           |{"name":"Berta", "salary":4000}
                                         """.stripMargin.split("\n"))
    val df = spark.read.json(rdd)
    df.createOrReplaceTempView("employees")
    df.show()

    val result =
      spark.sql("SELECT myAverage(salary) as average_salary FROM employees")
    result.show()
  }

}
