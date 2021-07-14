1. Tạo file DataTracking.proto với nội dung như đề bài.
   
2. Từ file .proto đó, ta thực hiện gen ra các file java : 1 class DataTracking.java, 1 class DataTrackingProtos.java, 1 interface DataTrackingOrBuilder.java
Câu lệnh: protoc -I=src/main/java --java_out=src/main/java src/main/java/week6/structuredStreaming/DataTracking.proto
   
3. Class ProducerPushDataToKafka để đẩy dữ liệu 10000 bảng ghi lên kafka

4. Class ConsumerDataFromKafka để consume dữ liệu từ kafka

5. Class JobStructuredStreaming lấy dữ liệu từ kafka đẩy vào HDFS. partition by year, month, day, hour.

6. Cấu hình Hive trên server2 (10.140.0.5)
Chạy 2 câu lệnh dưới để tạo hive table:
hive>CREATE EXTERNAL TABLE datatrackingtable(`Version` string,`Name` string,`Timestamp` string,`PhoneId` string, `Lon` string, `Lat` string) PARTITIONED BY (Year int, Month int, Day int, Hour int) STORED AS PARQUET LOCATION '/user/ngocpv22/data_tracking/data';
hive>MSCK REPAIR TABLE datatrackingtable;
   
=> Có thể truy vấn sql trên datatrackingtable hive table.