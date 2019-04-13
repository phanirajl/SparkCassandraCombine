package com.roee.cassandra.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.cassandra.*;
import  org.apache.spark.sql.cassandra.DefaultSource;
import org.apache.spark.sql.SparkSession;

import java.util.HashMap;
import java.util.Map;

public class CassandraToDataframe {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("dasd")
                .set("spark.cassandra.connection.host", "localhost")
                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .set("spark.cassandra.concurrent.reads", "70")
                .set("spark.cassandra.input.fetch.size_in_rows", "6000")
                .set("spark.cassandra.input.reads_per_sec", "70");
        JavaSparkContext sc = new JavaSparkContext(conf);


        SparkSession sparkSession = SparkSession.builder().config(conf).getOrCreate();

        Map<String,String> cassandraDetails = new HashMap<>();

        cassandraDetails.put("table","words");
        cassandraDetails.put("keyspace","test");
        Dataset<Row> df = sparkSession.read()
                .format("org.apache.spark.sql.cassandra")
                .options(cassandraDetails)
                .load();

        cassandraDetails.put("table","words2");
        cassandraDetails.put("keyspace","test");

        df.write().format("org.apache.spark.sql.cassandra")
                .option("confirm.truncate","true")
                .options(cassandraDetails)
                .mode(SaveMode.Overwrite)
                .saveAsTable("newtable");

    }
}
