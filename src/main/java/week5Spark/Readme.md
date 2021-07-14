Kết quả bài tập spark batch:
1. device_model_num_user: Path: hdfs://10.140.0.5:9000/user/ngocpv22/device_model_num_user
   Câu lệnh:
   hadoop jar /opt/temp/parquet-tools-1.9.0.jar cat --json hdfs:/user/ngocpv22/device_model_num_user/part-00000-95be4861-d5fc-49be-ab91-9f49990b8409-c000.snappy.parquet

2. device_model_list_user: Path: hdfs://10.140.0.5:9000/user/ngocpv22/device_model_list_user 
   Câu lệnh:
   hive --orcfiledump -d hdfs:/user/ngocpv22/device_model_list_user/part-00000-cb08db1d-bf20-4099-bb2f-3a680c63c4d2-c000.snappy.orc

3. button_count_by_user_id: hdfs://10.140.0.5:9000/user/ngocpv22/button_count_by_user_id_device_model 
   Câu lệnh:
   hadoop jar /opt/temp/parquet-tools-1.9.0.jar cat --json hdfs:/user/ngocpv22/button_count_by_user_id_device_model/part-00000-434ef1a8-5fe9-433e-bfff-122feb5dc1e8-c000.snappy.parquet
   
