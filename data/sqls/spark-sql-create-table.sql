CREATE DATABASE IF NOT EXISTS spark_test;
USE spark_test;

-- create table using spark sql syntax
CREATE TABLE IF NOT EXISTS users_csv(
    id INT,
    name STRING,
    age INT,
    is_vip BOOLEAN)
USING CSV;

CREATE TABLE IF NOT EXISTS users_parquet(
    id INT,
    name STRING,
    age INT,
    is_vip BOOLEAN)
USING PARQUET
OPTIONS('compression'='snappy');

INSERT OVERWRITE TABLE users_parquet VALUES
(1, 'Bob', 24, TRUE),
(2, 'Jack', 15, FALSE),
(3, 'Alice', 36, TRUE),
(4, 'Billy', 50, FALSE);

CREATE TABLE IF NOT EXISTS users_partitioned
USING ORC
PARTITIONED BY(age)
CLUSTERED BY(id) INTO 8 BUCKETS
AS SELECT * FROM users_parquet

-- column order changed
DESC users_partitioned

INSERT OVERWRITE TABLE users_partitioned VALUES
(1, 'Bob', TRUE, 24),
(2, 'Jack', FALSE, 15),
(3, 'Alice', TRUE, 36),
(4, 'Billy', FALSE, 36);

-- create table using hive syntax
CREATE TABLE IF NOT EXISTS users_hive(
    id INT,
    name STRING,
    age INT,
    is_vip BOOLEAN)
COMMENT 'This table is stored in hive'
STORED AS PARQUET
TBLPROPERTIES ('compression'='snappy')

INSERT OVERWRITE TABLE users_hive VALUES
(1, 'Bob', 24, TRUE),
(2, 'Jack', 15, FALSE),
(3, 'Alice', 36, TRUE),
(4, 'Billy', 50, FALSE);

INSERT OVERWRITE TABLE users_hive
SELECT * FROM users_csv

CREATE TABLE IF NOT EXISTS users_hive_txt(
    id INT,
    name STRING,
    age INT,
    is_vip BOOLEAN)
ROW FORMAT DELIMITED
    FIELDS TERMINATED BY ','
    LINES TERMINATED BY '\n'
STORED AS TEXTFILE

LOAD DATA LOCAL INPATH '/home/bigdata/users.csv'
OVERWRITE INTO TABLE users_hive_txt;

CREATE EXTERNAL TABLE IF NOT EXISTS users_hive_ext(
    id INT,
    name STRING,
    age INT,
    is_vip BOOLEAN)
ROW FORMAT DELIMITED
    FIELDS TERMINATED BY ','
    LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION 'hdfs://bigdata:9000/warehouse/spark_test.db/users_hive_txt'