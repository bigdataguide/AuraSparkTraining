package org.training.spark.sql

import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession

object MultipleDimensionAggregation {

  def main(args: Array[String]) {
    val spark = SparkSession
      .builder()
      .appName("MultipleDimensionAggregation")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    import spark.implicits._

    val sales = Seq(
      ("Warsaw", 2016, 100),
      ("Warsaw", 2017, 200),
      ("Boston", 2015, 50),
      ("Boston", 2016, 150),
      ("Toronto", 2017, 50)
    ).toDF("city", "year", "amount")

    val groupByCityAndYear = sales
      .groupBy("city", "year")
      .agg(sum("amount") as "amount")
      .orderBy($"city".desc_nulls_last, $"year".asc_nulls_last)
    println("group by city and year")
    groupByCityAndYear.show()

    val withRollup = sales
      .rollup("city", "year")
      .agg(sum("amount") as "amount", grouping_id() as "gid")
      .sort($"city".desc_nulls_last, $"year".asc_nulls_last)
      .filter(grouping_id() =!= 3)
      .select("city", "year", "amount")
    println("rollup city and year")
    withRollup.show()

    val withCube = sales.cube("city", "year")
      .agg(sum("amount") as "amount")
      .sort($"city".desc_nulls_last, $"year".asc_nulls_last)
    println("cube city and year")
    withCube.show()

    val withPivot = sales.groupBy("city")
      .pivot("year", Seq(2015,2016,2017))
      .agg(sum("amount") as "amount")
      .sort($"city".desc)
    println("group by city and pivot by year")
    withPivot.show()

//    val groupByCityOnly = sales
//      .groupBy("city")
//      .agg(sum("amount") as "amount")
//      .select($"city", lit(null) as "year", $"amount")
//    groupByCityOnly.show()
//
//    val withUnion = groupByCityAndYear
//      .union(groupByCityOnly)
//      .sort($"city".desc_nulls_last, $"year".desc_nulls_last)
//    withUnion.show()

//    sales.createOrReplaceTempView("sales")
//
//    val withGroupingSets =
//      spark.sql("""
//        |SELECT city, year, SUM(amount) as amount
//        |FROM sales
//        |GROUP BY city, year
//        |GROUPING SETS ((city, year), (city))
//        |ORDER BY city DESC NULLS LAST, year ASC NULLS LAST
//      """.stripMargin)
//    withGroupingSets.show()
//
//    val q = sales
//      .rollup("city", "year")
//      .agg(sum("amount") as "amount")
//      .sort($"city".desc_nulls_last, $"year".asc_nulls_last)
//    q.show()
//
//    val q1 = sales
//      .groupBy("city", "year") // <-- subtotals (city, year)
//      .agg(sum("amount") as "amount")
//    val q2 = sales
//      .groupBy("city") // <-- subtotals (city)
//      .agg(sum("amount") as "amount")
//      .select($"city", lit(null) as "year", $"amount") // <-- year is null
//    val q3 = sales
//      .groupBy() // <-- grand total
//      .agg(sum("amount") as "amount")
//      .select(lit(null) as "city", lit(null) as "year", $"amount") // <-- city and year are null
//    val qq = q1
//      .union(q2)
//      .union(q3)
//      .sort($"city".desc_nulls_last, $"year".asc_nulls_last)
//    qq.show()
  }

}
