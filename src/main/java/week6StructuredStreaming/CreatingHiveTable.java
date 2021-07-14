package week6StructuredStreaming;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

public class CreatingHiveTable {
    public static void main(String[] args){
        // warehouseLocation points to the default location for managed databases and tables

        SparkSession spark = SparkSession
                .builder()
                .master("local")
                .appName("Java Spark Streaming data from kafka")
                .getOrCreate();

        Dataset<Row> df= spark.read().parquet("/path/to/_common_metadata");

        String ddl1 = "CREATE EXTERNAL TABLE "+ "tableName"
                + " () "
                + "STORED AS PARQUET LOCATION '/wherever/you/store/the/data/'" ;

    }
}
