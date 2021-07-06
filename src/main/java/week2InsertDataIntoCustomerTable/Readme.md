Insert 1 lúc 5M records
Mỗi lần excute là insert 10 nghìn bản ghi vaò csdl, lặp lại đến đủ 5M

Thời gian gen dữ liệu: 2 phút
Thời gian đọc từ file csv -> insert vào database: 118 phút (gần 2 tiếng)

//Solution từ anh Hòa:
dùng jdbc batch insert + disable autocommit + rewriteBatchedStatements=true giúp tăng tốc độ insert
import java.io.BufferedReader;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

public class InsertRefactor {
public static void main(String[] args)
{
try
{
int count = 0 ;
long startTime = System.nanoTime();

            String myUrl = "jdbc:mysql://localhost:3306/test?useSSL=false&rewriteBatchedStatements=true";
            Connection conn = DriverManager.getConnection(myUrl, "test", "test");
            conn.setAutoCommit(false);

            String query = " insert into customers_packages (pkg_order, shop_code, customer_tel, customer_tel_normalize, fullname, pkg_created, pkg_modified, package_status_id, customer_province_id, customer_district_id, customer_ward_id, is_cancel, ightk_user_id)"
                    + " values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
            int BATCH_SIZE = 1000;

            PreparedStatement preparedStmt = conn.prepareStatement(query);

            BufferedReader lineReader = new BufferedReader(new FileReader("data/data.csv"));
            String lineText = null;
            lineReader.readLine();
            while ((lineText = lineReader.readLine()) != null) {
                String[] data = lineText.split(",");

                String pkg_order = data[0];
                String shop_code = data[1];;
                String customer_tel = data[2];
                String customer_tel_normalize = data[3];
                String fullname = data[4];
                String pkg_created = data[5];
                String pkg_modified = data[6];

                String package_status_id = data[7];

                String customer_province_id = data[8];


                String customer_district_id = data[9];


                String customer_ward_id = data[10];

                String is_cancel = data[11];

                String ightk_user_id = data[12];

                preparedStmt.setString(1, pkg_order);
                preparedStmt.setString(2, shop_code);
                preparedStmt.setString(3, customer_tel);
                preparedStmt.setString(4, customer_tel_normalize);
                preparedStmt.setString(5, fullname);
                preparedStmt.setString(6, pkg_created);
                preparedStmt.setString(7, pkg_modified);
                preparedStmt.setString(8, package_status_id);
                preparedStmt.setString(9, customer_province_id);
                preparedStmt.setString(10, customer_district_id);
                preparedStmt.setString(11, customer_ward_id);
                preparedStmt.setString(12, is_cancel);
                preparedStmt.setString(13, ightk_user_id);
                preparedStmt.addBatch();
                count ++;
                if (count % BATCH_SIZE == 0) {
                    preparedStmt.executeBatch();
                    conn.commit();
                    System.out.println("insert batch " + count/BATCH_SIZE + " th");
                }
            }
            long endTime = System.nanoTime();

            long duration = (endTime - startTime);
            double durationSec = (double) duration / 1_000_000_000;
            System.out.println(durationSec);
            conn.close();
        }
        catch (Exception e)
        {
            System.err.println("Got an exception!");
            System.err.println(e.getMessage());
        }
    }
}