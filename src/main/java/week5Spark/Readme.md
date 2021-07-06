Kết quả bài tập spark batch:
1. device_model_num_user: Path: hdfs://10.140.0.5:9000/user/ngocpv22/device_model_num_user
   Câu lệnh:
   hadoop jar /opt/temp/parquet-tools-1.9.0.jar cat --json hdfs:/user/ngocpv22/device_model_num_user/part-00000-d931a368-b3e9-43fd-aeac-81e273143b8a-c000.snappy.parquet

2. device_model_list_user: Path: hdfs://10.140.0.5:9000/user/ngocpv22/device_model_list_user 
   Câu lệnh:
   hive --orcfiledump -d hdfs:/user/ngocpv22/device_model_list_user/part-00000-dd0fd09f-7634-46d7-95de-421e9e3923ec-c000.snappy.orc

3. button_count_by_user_id: hdfs://10.140.0.5:9000/user/ngocpv22/button_count_by_user_id_device_model 
   Câu lệnh:
   hadoop jar /opt/temp/parquet-tools-1.9.0.jar cat --json hdfs:/user/ngocpv22/button_count_by_user_id_device_model/part-00000-ccbf512e-6e60-4e0b-a492-9d2c77eb6169-c000.snappy.parquet
   
