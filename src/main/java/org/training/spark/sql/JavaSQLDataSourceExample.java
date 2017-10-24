package org.training.spark.sql;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class JavaSQLDataSourceExample {
  public static void main(String[] args) {
    SparkSession spark = SparkSession
      .builder()
      .appName("Java Spark SQL data sources example")
      .config("spark.some.config.option", "some-value")
      .enableHiveSupport()
      .getOrCreate();

    runBasicDataSourceExample(spark);
    runBasicParquetExample(spark);
    runOtherDatasetExample(spark);

    spark.stop();
  }

  private static void runBasicDataSourceExample(SparkSession spark) {
    Dataset<Row> peopleDF =
      spark.read().format("json").load("data/ml-1m/users.json");
    peopleDF.select("userID", "age").write()
            .mode("overwrite")
            .format("parquet")
            .save("namesAndAges.parquet");

    Dataset<Row> sqlDF =
      spark.sql("SELECT * FROM parquet.`namesAndAges.parquet`");

    Dataset<Row> usersDF = spark.read().load("data/ml-1m/users.parquet");
    usersDF.select("userID", "age").show();
  }

  private static void runBasicParquetExample(SparkSession spark) {
    // Read in the Parquet file created above.
    // Parquet files are self-describing so the schema is preserved
    // The result of loading a parquet file is also a DataFrame
    Dataset<Row> parquetFileDF = spark.read().parquet("data/ml-1m/users.parquet");

    // Parquet files can also be used to create a temporary view and then used in SQL statements
    parquetFileDF.createOrReplaceTempView("users");
    Dataset<Row> namesDF = spark.sql("SELECT userID FROM users WHERE age BETWEEN 13 AND 19");
    Dataset<Long> namesDS = namesDF.map(new MapFunction<Row, Long>() {
      public Long call(Row row) {
        return row.getLong(0);
      }
    }, Encoders.LONG());
    namesDS.show();
  }

  private static void runOtherDatasetExample(SparkSession spark) {
    Dataset<Row> parquetFileDF = spark.read().parquet("data/ml-1m/users.parquet");

    parquetFileDF.write().mode("overwrite").csv("user.csv");
    Dataset<Row> csvFileDF = spark.read().csv("user.csv");
    csvFileDF.show();

    parquetFileDF.write().mode("overwrite").orc("user.orc");
    Dataset<Row> orcFileDF = spark.read().orc("user.orc");
    orcFileDF.show();
  }
}