package org.training.spark.sql;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import java.io.Serializable;
import java.util.*;

import static org.apache.spark.sql.functions.*;

public class JavaSparkSQLExample {
    public static class User implements Serializable {
        private long userID;
        private String gender;
        private int age;
        private String occupation;
        private String zipcode;

        public long getUserID() {
            return userID;
        }

        public void setUserID(long userID) {
            this.userID = userID;
        }

        public String getGender() {
            return gender;
        }

        public void setGender(String gender) {
            this.gender = gender;
        }

        public int getAge() {
            return age;
        }

        public void setAge(int age) {
            this.age = age;
        }

        public String getOccupation() {
            return occupation;
        }

        public void setOccupation(String occupation) {
            this.occupation = occupation;
        }

        public String getZipcode() {
            return zipcode;
        }

        public void setZipcode(String zipcode) {
            this.zipcode = zipcode;
        }
    }

    public static void main(String[] args) throws AnalysisException {
        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark SQL basic example")
                .config("spark.some.config.option", "some-value")
                .getOrCreate();

        runBasicDataFrameExample(spark);
        runDatasetCreationExample(spark);
        runInferSchemaExample(spark);
        runProgrammaticSchemaExample(spark);

        spark.stop();
    }

    private static void runBasicDataFrameExample(SparkSession spark)
            throws AnalysisException {
        Dataset<Row> df = spark.read().json("data/ml-1m/users.json");

        // Displays the content of the DataFrame to stdout
        df.show();

        // Print the schema in a tree format
        df.printSchema();

        // Select only the "name" column
        df.select("userID").show();

        // Select everybody, but increment the age by 1
        df.select(col("userID"), col("age").plus(1)).show();
        df.select(count("occupation"), max("age"), min("age")).show();

        // Select people older than 21
        df.filter(col("age").gt(21)).show();

        // Count people by age
        df.groupBy("age").count().show();

        df.groupBy("occupation").agg(max("age"), min("age")).show();

        // Register the DataFrame as a SQL temporary view
        df.createOrReplaceTempView("user");

        Dataset<Row> sqlDF = spark.sql("SELECT * FROM user");
        sqlDF.show();

        // Register the DataFrame as a global temporary view
        df.createGlobalTempView("people");

        // Global temporary view is tied to a system preserved database `global_temp`
        spark.sql("SELECT * FROM global_temp.people").show();

        // Global temporary view is cross-session
        spark.newSession().sql("SELECT * FROM global_temp.people").show();
    }

    private static void runDatasetCreationExample(SparkSession spark) {
        // $example on:create_ds$
        // Create an instance of a Bean class
        User user = new User();
        user.setUserID(123L);
        user.setAge(32);
        user.setGender("m");
        user.setOccupation("12");
        user.setZipcode("12345");

        // Encoders are created for Java beans
        Encoder<User> userEncoder = Encoders.bean(User.class);
        Dataset<User> javaBeanDS = spark.createDataset(
                Collections.singletonList(user),
                userEncoder
        );
        javaBeanDS.show();

        // Encoders for most common types are provided in class Encoders
        Encoder<Integer> integerEncoder = Encoders.INT();
        Dataset<Integer> primitiveDS = spark.createDataset(Arrays.asList(1, 2, 3), integerEncoder);
        Dataset<Integer> transformedDS = primitiveDS.map(new MapFunction<Integer, Integer>() {
            @Override
            public Integer call(Integer value) throws Exception {
                return value + 1;
            }
        }, integerEncoder);
        transformedDS.collect(); // Returns [2, 3, 4]

        // DataFrames can be converted to a Dataset by providing a class. Mapping based on name
        String path = "data/ml-1m/users.json";
        Dataset<User> peopleDS = spark.read().json(path).as(userEncoder);
        peopleDS.show();
    }

    private static void runInferSchemaExample(SparkSession spark) {
        // Create an RDD of Person objects from a text file
        JavaRDD<User> userRDD = spark.read()
                .textFile("data/ml-1m/users.dat")
                .javaRDD()
                .map(new Function<String, User>() {
                    @Override
                    public User call(String line) throws Exception {
                        String[] parts = line.split("::");
                        User person = new User();
                        person.setUserID(Long.parseLong(parts[0]));
                        person.setGender(parts[1]);
                        person.setAge(Integer.parseInt(parts[2].trim()));
                        person.setOccupation(parts[3]);
                        person.setZipcode(parts[4]);
                        return person;
                    }
                });

        // Apply a schema to an RDD of JavaBeans to get a DataFrame
        Dataset<Row> userDF = spark.createDataFrame(userRDD, User.class);
        // Register the DataFrame as a temporary view
        userDF.createOrReplaceTempView("user");

        // SQL statements can be run by using the sql methods provided by spark
        Dataset<Row> teenagersDF = spark.sql("SELECT userID FROM user WHERE age BETWEEN 13 AND 19");

        // The columns of a row in the result can be accessed by field index
        Encoder<Long> longEncoder = Encoders.LONG();
        Dataset<Long> teenagerIDByIndexDF = teenagersDF.map(new MapFunction<Row, Long>() {
            @Override
            public Long call(Row row) throws Exception {
                return row.getLong(0);
            }
        }, longEncoder);
        teenagerIDByIndexDF.show();

        // or by field name
        Dataset<Long> teenagerNamesByFieldDF = teenagersDF.map(new MapFunction<Row, Long>() {
            @Override
            public Long call(Row row) throws Exception {
                return row.<Long>getAs("userID");
            }
        }, longEncoder);
        teenagerNamesByFieldDF.show();

    }

    private static void runProgrammaticSchemaExample(SparkSession spark) {
        // Create an RDD
        JavaRDD<String> peopleRDD = spark.sparkContext()
                .textFile("data/ml-1m/users.dat", 1)
                .toJavaRDD();

        // The schema is encoded in a string
        String schemaString = "userID gender age occupation zipcode";

        // Generate the schema based on the string of schema
        List<StructField> fields = new ArrayList<>();
        for (String fieldName : schemaString.split(" ")) {
            StructField field = DataTypes.createStructField(fieldName, DataTypes.StringType, true);
            fields.add(field);
        }
        StructType schema = DataTypes.createStructType(fields);

        // Convert records of the RDD (people) to Rows
        JavaRDD<Row> rowRDD = peopleRDD.map(new Function<String, Row>() {
            @Override
            public Row call(String record) throws Exception {
                String[] attributes = record.split("::");
                return RowFactory.create(attributes);
            }
        });

        // Apply the schema to the RDD
        Dataset<Row> userDataFrame = spark.createDataFrame(rowRDD, schema);

        userDataFrame.filter(userDataFrame.col("age").gt(20)).show();
        userDataFrame.filter("age > 20").show();

        // Creates a temporary view using the DataFrame
        userDataFrame.createOrReplaceTempView("user");

        // SQL can be run over a temporary view created using DataFrames
        Dataset<Row> results = spark.sql("SELECT userID FROM user");

        // The results of SQL queries are DataFrames and support all the normal RDD operations
        // The columns of a row in the result can be accessed by field index or by field name
        Dataset<String> userIDDS = results.map(new MapFunction<Row, String>() {
            @Override
            public String call(Row row) throws Exception {
                return "ID: " + row.getString(0);
            }
        }, Encoders.STRING());
        userIDDS.show();
    }
}