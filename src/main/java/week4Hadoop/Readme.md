ngocpv22 - Mô tả ngắn về bài tập code java Hadoop
Bài 1: Word Count
File run: WordCount.java
output ở : hdfs dfs -ls /user/ngocpv22/wordcount/output

Bài 2:
File run: NumCount.java
Để tìm được số lượng các số distinct có trong file data.txt, ta thực hiện việc tăng dần biến đếm (wordCount) ở mỗi reduce phase
(Số lượng reduce phase = số lượng số distinct)
Điều kiện: job.setNumReduceTasks(1); (Set số lượng job reduce = 1 thì biến đếm wordCount tăng dần mới chính xác)
output ở : hdfs dfs -ls /user/ngocpv22/numcount/output

Bài 3:
File run: JoinTable.java
Tiến trình được mô tả trong ảnh đính kèm: MapReduceJoinTableProcess.png
Mô tả: Sử dụng cách 3 anh Tùng gợi ý:
- Sử dụng 2 pha map cho 2 table: people.csv và salary.csv
  2 pha map này đều write ra kiểu dữ liệu: (ProfessionKey, JoinGenericWritable) (Vì pha reduce cần 1 kiểu chung nên ta cần phải có 1 cái JoinGenericWritable extends từ GenericWritable)
- Set recordType cho ProfessionKey của Salary là 0, của People là 1
  => Hàm JoinSortingComparator có nhiệm vụ đảm bảo bảo 1 bản ghi Salary sẽ đến trước, sau đó là các bản ghi People (Hàm compareTo(ProfessionKey other) so sánh 2 recordType)
  Như vậy, tại pha reduce, ta chỉ cần lấy ra salary (phần tử đầu của values (input value)) rồi append vào các bản ghi people là xong.
- Ta đã bỏ qua header ở file csv trong mỗi pha map => tại pha reduce, thêm hàm setup(Context context) để thêm header cho output.
  output ở : hdfs dfs -ls /user/ngocpv22/joinTable/output