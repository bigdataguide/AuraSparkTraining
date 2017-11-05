package org.training.spark.sql

import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession

case class DeviceData(id: Int, device: String)

object MyJsonFunction {

  def main(args: Array[String]) {
    val spark = SparkSession
      .builder()
      .appName("MyJsonFunction")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val rdd = spark.sparkContext
      .textFile("data/textfile/events.logs")
      .zipWithIndex()
      .map(x => (x._2.toInt, x._1))
    val eventsDS = rdd.toDF("id", "device").as[DeviceData]
    eventsDS.show()
    eventsDS.printSchema()

    val eventsFromJSONDF = rdd.toDF("id", "json")
    eventsFromJSONDF.show()

    val jsDF = eventsFromJSONDF.select(
      $"id",
      get_json_object($"json", "$.device_type").alias("device_type"),
      get_json_object($"json", "$.ip").alias("ip"),
      get_json_object($"json", "$.cca3").alias("cca3")
    )
    jsDF.show()

    val jsonSchema = new StructType()
      .add("battery_level", LongType)
      .add("c02_level", LongType)
      .add("cca3", StringType)
      .add("cn", StringType)
      .add("device_id", LongType)
      .add("device_type", StringType)
      .add("signal", LongType)
      .add("ip", StringType)
      .add("temp", LongType)
      .add("timestamp", TimestampType)

    val devicesDF = eventsDS
      .select(from_json($"device", jsonSchema) as "devices")
      .select($"devices.*")
      .filter($"devices.temp" > 10 and $"devices.signal" > 15)
    devicesDF.show()

    val devicesUSDF = devicesDF
      .select($"*")
      .where($"cca3" === "USA")
      .orderBy($"signal".desc, $"temp".desc)
    devicesUSDF.show()

    val stringJsonDF = eventsDS.select(to_json(struct($"*"))).toDF("devices")
    stringJsonDF.show()

    devicesDF
      .selectExpr("c02_level", "round(c02_level/temp) as ratio_c02_temperature")
      .orderBy($"ratio_c02_temperature" desc)
      .show()

    devicesDF.createOrReplaceTempView("devicesDFT")

    spark.sql("""
        |select
        | c02_level,
        | round(c02_level/temp) as ratio_c02_temperature
        |from devicesDFT
        |order by ratio_c02_temperature desc
      """.stripMargin).show()

    stringJsonDF
      .write
      .mode("overwrite")
      .format("parquet")
      .save("/tmp/jules")

    val parquetDF = spark.read.parquet("/tmp/jules")
    parquetDF.printSchema()
    parquetDF.show()
  }

}
