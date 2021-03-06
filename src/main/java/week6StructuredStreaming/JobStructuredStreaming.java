package week6StructuredStreaming;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.concurrent.TimeoutException;
import java.sql.Timestamp;
import java.util.Date;

public class JobStructuredStreaming {
    public static void main(String[] args) throws StreamingQueryException, TimeoutException {
        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark Streaming data from kafka")
                .getOrCreate();

        Dataset<Row> df = spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "10.140.0.3:9092")
                .option("subscribe", "data_tracking_ngocpv22")
                .option("group.id","group1")
                .option("startingOffsets", "earliest")
                .option("auto.offset.reset","true")
                .load();

        Dataset<byte[]> df1 = df.select("value").as(Encoders.BINARY());

        Dataset<String> df2 = df1.map((MapFunction<byte[], String>)
                    s ->  Message.DataTracking.parseFrom(s).getVersion() + ","
                        + Message.DataTracking.parseFrom(s).getName() + ","
                        + Message.DataTracking.parseFrom(s).getTimestamp() + ","
                        + Message.DataTracking.parseFrom(s).getPhoneId() + ","
                        + Message.DataTracking.parseFrom(s).getLon() + ","
                        + Message.DataTracking.parseFrom(s).getLat() + ","

                        + getTime(Message.DataTracking.parseFrom(s).getTimestamp(), "yyyy") + ","
                        + getTime(Message.DataTracking.parseFrom(s).getTimestamp(), "MM") + ","
                        + getTime(Message.DataTracking.parseFrom(s).getTimestamp(), "dd") + ","
                        + getTime(Message.DataTracking.parseFrom(s).getTimestamp(), "HH")
                , Encoders.STRING());

        Dataset<Row> df3 = df2.withColumn("version", functions.split(df2.col("value"), ",").getItem(0))
                .withColumn("name", functions.split(df2.col("value"), ",").getItem(1))
                .withColumn("timestamp", functions.split(df2.col("value"), ",").getItem(2))
                .withColumn("phoneId", functions.split(df2.col("value"), ",").getItem(3))
                .withColumn("lon", functions.split(df2.col("value"), ",").getItem(4))
                .withColumn("lat", functions.split(df2.col("value"), ",").getItem(5))
                .withColumn("year", functions.split(df2.col("value"), ",").getItem(6))
                .withColumn("month", functions.split(df2.col("value"), ",").getItem(7))
                .withColumn("day", functions.split(df2.col("value"), ",").getItem(8))
                .withColumn("hour", functions.split(df2.col("value"), ",").getItem(9))
                .drop("value");

//        StreamingQuery query = df3
//                .writeStream()
//                .outputMode("update")
//                .format("console")
//                .start();

        StreamingQuery query = df3
                .writeStream()
                .outputMode("append")
                .format("parquet")
                .option("checkpointLocation","hdfs://10.140.0.5:9000/user/ngocpv22/data_tracking/checkpoint")
                .option("compression","snappy")
                .partitionBy("year", "month", "day", "hour")
                .option("path", "hdfs://10.140.0.5:9000/user/ngocpv22/data_tracking/data")
                .start();

        query.awaitTermination();
        spark.streams().awaitAnyTermination(2000);
    }
    public static String getTime(long timestamp, String getBy){
        Date date = new java.util.Date(timestamp*1000L);
        SimpleDateFormat sdf = new java.text.SimpleDateFormat(getBy);
        sdf.setTimeZone(java.util.TimeZone.getTimeZone("GMT+07:00"));
        String formattedDate = sdf.format(date);
        return formattedDate;
    }
}

