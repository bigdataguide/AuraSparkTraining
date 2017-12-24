package org.training.spark.ml;

import org.apache.spark.sql.SparkSession;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.ml.feature.OneHotEncoder;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.StringIndexerModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class JavaOneHotEncoderExample {
  public static void main(String[] args) {
    SparkSession spark = SparkSession
        .builder()
        .appName("JavaOneHotEncoderExample")
        .getOrCreate();

    List<Row> data = Arrays.asList(
        RowFactory.create(0, "a"),
        RowFactory.create(1, "b"),
        RowFactory.create(2, "c"),
        RowFactory.create(3, "a"),
        RowFactory.create(4, "a"),
        RowFactory.create(5, "c")
    );

    StructType schema = new StructType(new StructField[]{
        new StructField("id", DataTypes.IntegerType, false, Metadata.empty()),
        new StructField("category", DataTypes.StringType, false, Metadata.empty())
    });

    Dataset<Row> df = spark.createDataFrame(data, schema);

    StringIndexerModel indexer = new StringIndexer()
        .setInputCol("category")
        .setOutputCol("categoryIndex")
        .fit(df);
    Dataset<Row> indexed = indexer.transform(df);

    OneHotEncoder encoder = new OneHotEncoder()
        .setInputCol("categoryIndex")
        .setOutputCol("categoryVec");

    Dataset<Row> encoded = encoder.transform(indexed);
    encoded.show();

    spark.stop();
  }
}

