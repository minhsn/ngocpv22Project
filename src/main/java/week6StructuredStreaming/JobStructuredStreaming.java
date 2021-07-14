package week6StructuredStreaming;

import org.apache.spark.api.java.function.MapFunction;
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
                .master("local")
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

                        + getYear(Message.DataTracking.parseFrom(s).getTimestamp()) + ","
                        + getMonth(Message.DataTracking.parseFrom(s).getTimestamp()) + ","
                        + getDay(Message.DataTracking.parseFrom(s).getTimestamp()) + ","
                        + getHour(Message.DataTracking.parseFrom(s).getTimestamp())

                , Encoders.STRING());

        Dataset<Row> df3 = df2.withColumn("Version", functions.split(df2.col("value"), ",").getItem(0))
                .withColumn("Name", functions.split(df2.col("value"), ",").getItem(1))
                .withColumn("Timestamp", functions.split(df2.col("value"), ",").getItem(2))
                .withColumn("PhoneId", functions.split(df2.col("value"), ",").getItem(3))
                .withColumn("Lon", functions.split(df2.col("value"), ",").getItem(4))
                .withColumn("Lat", functions.split(df2.col("value"), ",").getItem(5))
                .withColumn("Year", functions.split(df2.col("value"), ",").getItem(6))
                .withColumn("Month", functions.split(df2.col("value"), ",").getItem(7))
                .withColumn("Day", functions.split(df2.col("value"), ",").getItem(8))
                .withColumn("Hour", functions.split(df2.col("value"), ",").getItem(9))
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
                .partitionBy("Year", "Month", "Day", "Hour")
                .option("path", "hdfs://10.140.0.5:9000/user/ngocpv22/data_tracking/data")
                .start();

        query.awaitTermination();
        spark.streams().awaitAnyTermination(2000);
    }

    public static String getYear(long timeStamp){
        Date date = new java.util.Date(timeStamp*1000L);
        SimpleDateFormat sdf = new java.text.SimpleDateFormat("yyyy");
        //SimpleDateFormat sdf = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss z");
        sdf.setTimeZone(java.util.TimeZone.getTimeZone("GMT+07:00"));
        String formattedDate = sdf.format(date);
        return formattedDate;
    }
    public static String getMonth(long timeStamp){
        Date date = new java.util.Date(timeStamp*1000L);
        SimpleDateFormat sdf = new java.text.SimpleDateFormat("MM");
        sdf.setTimeZone(java.util.TimeZone.getTimeZone("GMT+07:00"));
        String formattedDate = sdf.format(date);
        return formattedDate;
    }
    public static String getDay(long timeStamp){
        Date date = new java.util.Date(timeStamp*1000L);
        SimpleDateFormat sdf = new java.text.SimpleDateFormat("dd");
        sdf.setTimeZone(java.util.TimeZone.getTimeZone("GMT+07:00"));
        String formattedDate = sdf.format(date);
        return formattedDate;
    }
    public static String getHour(long timeStamp){
        Date date = new java.util.Date(timeStamp*1000L);
        SimpleDateFormat sdf = new java.text.SimpleDateFormat("HH");
        sdf.setTimeZone(java.util.TimeZone.getTimeZone("GMT+07:00"));
        String formattedDate = sdf.format(date);
        return formattedDate;
    }

}

