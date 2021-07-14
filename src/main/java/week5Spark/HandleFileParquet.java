package week5Spark;

import org.apache.spark.sql.*;

import static org.apache.spark.sql.functions.*;
import static org.apache.spark.sql.functions.approx_count_distinct;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.collect_set;
import static org.apache.spark.sql.functions.concat;
import static org.apache.spark.sql.functions.count;
import static org.apache.spark.sql.functions.lit;

public class HandleFileParquet {
    public static void main(String[] args) {
        //Create a basic SparkSession
        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark SQL handle parquet file")
                .config("spark.some.config.option", "some-value")
                .getOrCreate();

        Dataset<Row> dataframe1 = spark.read().parquet("/Sample_data");
        //Loai bỏ các bản tin có chứa device_model và user_id là null
        dataframe1 = dataframe1.filter("device_model <> ''").filter("user_id <> ''");

        //Group data lại theo device_model và count số người dùng duy nhất
        Dataset<Row> device_model_num_user = dataframe1.groupBy("device_model").agg(approx_count_distinct("user_id")).withColumnRenamed("approx_count_distinct(user_id)", "count");
        device_model_num_user.coalesce(1).write().mode(SaveMode.Overwrite).option("compression","snappy").parquet("hdfs:/user/ngocpv22/device_model_num_user");
        //Group data lại theo device_model và list user duy nhất
        Dataset<Row> device_model_list_user = dataframe1.groupBy("device_model").agg(collect_set("user_id")).withColumnRenamed("collect_set(user_id)", "list_user_id");
        device_model_list_user.coalesce(1).write().mode(SaveMode.Overwrite).option("compression","snappy").orc("hdfs:/user/ngocpv22/device_model_list_user");
        //device_model_list_user.repartition(1)

        //Tạo dataframe mới chứa user_id_device_model với nội dung là user_id +_+device_model
        Dataset<Row> dataframe2 = spark.read().parquet("/Sample_data");
        //Loại bỏ các trường có buttion_id là NULL
        dataframe2 = dataframe2.filter("button_id <> ''");
        //Count số user_id_device_model , button_id thành dataframe action_by_button_id
        Dataset<Row> action_by_button_id = dataframe2.withColumn("user_id_device_model",concat(col("user_id"), lit("_"), col("device_model")));
        Dataset<Row> button_count_by_user_id_device_model = action_by_button_id.groupBy("user_id_device_model", "button_id").agg(count("user_id_device_model")).withColumnRenamed("count(user_id_device_model)", "count");

        button_count_by_user_id_device_model.coalesce(1).write().mode(SaveMode.Overwrite).option("compression","snappy").parquet("hdfs:/user/ngocpv22/button_count_by_user_id_device_model");
    }
}
