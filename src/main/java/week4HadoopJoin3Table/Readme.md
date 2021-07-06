Join 3 bảng: people, salary, score

- Thêm thuộc tính score vào bảng people.
- Tiến hành join 2 bảng people và salary tương tự như ở bài trước.
  Tuy nhiên, trong pha PeopleMapper, đọc file score.csv từ cache, lấy ra 1 hashMap: peopleScore
  score cho people sẽ được lấy bằng cách: score = peopleScore.get(id) với id là id của people đầu vào của pha PeopleMapper

*Logic ban đầu là như vậy, tuy nhiên khi em chạy chương trình bằng lệnh:
hadoop jar /opt/temp/ngocJoin3Table.jar /user/ngocpv22/joinTable/salary.csv /user/ngocpv22/joinTable/people.csv /opt/temp/score.csv /user/ngocpv22/join3Table/output
Tương ứng là args[0], args[1] : 2 file input, còn args[2] là file để addCacheFile, args[3] là output;

Thì Error:
Failing this attempt.Diagnostics: [2021-06-29 07:16:11.437]Failed to download resource { { file:/opt/temp/score.csv, 1624950865000, FILE, null },pending,[(container_1624864403800_0090_02_000001)],239730608852646,DOWNLOADING}
java.io.FileNotFoundException: File file:/opt/temp/score.csv does not exist

Khắc phục bằng cách: ở tòan bộ các node sẽ đều phải chứa file score.csv thi xuat hien loi:
Failing this attempt.Diagnostics: [2021-06-29 07:27:22.944]Failed to download resource { { file:/opt/temp/score.csv, 1624950865000, FILE, null },pending,[(container_1624864403800_0093_02_000001)],240373143604739,DOWNLOADING}
java.io.IOException: Resource file:/opt/temp/score.csv changed on src filesystem (expected 1624950865000, was 1624951563000

Em nghĩ đến trường hợp sẽ đọc file score.csv từ HDFS nhưng đọc như vậy sẽ đọc ra bytes nên chưa biết làm thế nào tiếp theo.
Anh cho em hỏi là, vậy khi join 3 bảng như vậy thì sẽ làm theo cách nào ạ?
Anh đọc được dòng này thì trả lời em trên Gchat với anh nhé ^-^