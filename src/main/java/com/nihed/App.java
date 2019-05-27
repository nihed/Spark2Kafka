package com.nihed;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;


/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args ) throws StreamingQueryException {
        SparkSession spark = SparkSession
                .builder()
                .appName("JavaStructured")
                .master("local[2]")
                .getOrCreate();


        StreamingQuery start = spark.readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "localhost:9092")
                .option("subscribe", "twitter")
                .option("auto.offset.reset", "earliest")
                .load()
                .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
                .writeStream()
                .format("console")
                .trigger(Trigger.ProcessingTime("1 second")).start();

        start.awaitTermination();
    }
}
