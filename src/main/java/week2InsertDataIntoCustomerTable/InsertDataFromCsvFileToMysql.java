package week2InsertDataIntoCustomerTable;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.*;
import org.fluttercode.datafactory.impl.DataFactory;

public class InsertDataFromCsvFileToMysql {

    private static String jdbcURL = "jdbc:mysql://localhost:3306/baitapsql";
    private static String username = "root";
    private static String password = "04112000";

    public static void main(String[] args) {
        String csvFilePath = "C:\\CustomProject\\ngocpv22Project\\ngocpv22MasterDevSeason3\\src\\main\\java\\week2InsertDataIntoCustomerTable\\Data.csv";
        int batchSize = 10000;
        Connection connection = null;

        try {
            //Tạo connect tới mysql
            connection = DriverManager.getConnection(jdbcURL, username, password);

            //Khai báo chuỗi query sẽ excute mỗi 10000 bản ghi
            String query = "INSERT INTO baitapsql.customers_packages " +
                    "(pkg_order, shop_code, customer_tel, customer_tel_normalize, fullname, pkg_created, pkg_modified, package_status_id, customer_province_id, customer_district_id, customer_ward_id, created, modified, is_cancel, ightk_user_id) " +
                    " VALUES ";

            BufferedReader lineReader = new BufferedReader(new FileReader(csvFilePath));
            String lineText = null;

            int count = 0;
            long timeStart = System.currentTimeMillis();

            Statement stmt = connection.createStatement();

            //Bỏ qua dòng header đầu tiên trong file
            lineReader.readLine();

            //Đọc từng dòng trong file csv sau đó cộng dồn vào chuỗi query
            while ((lineText = lineReader.readLine()) != null) {
                count ++;
                String[] data = lineText.split(",");


                query += "("+ data[0] + ", " + data[1] +", "+data[2] +", "+data[3]+", "+data[4]+", "+data[5]+", "+data[6]+", "+data[7]+", "+data[8]+", "+data[9]+", "+data[10]+", "+data[11]+", "+data[12]+", "+data[13]+ ", "+ data[14] +")";

                //Khi số lượng dòng đọc đủ 1 batch (10000 dòng) tiến hành excute
                if (count % batchSize == 0) {
                    stmt.executeUpdate(query);
                    query = "INSERT INTO baitapsql.customers_packages " +
                            "(pkg_order, shop_code, customer_tel, customer_tel_normalize, fullname, pkg_created, pkg_modified, package_status_id, customer_province_id, customer_district_id, customer_ward_id, created, modified, is_cancel, ightk_user_id) " +
                            " VALUES ";
                }else{
                    query += ",";
                }
            }
            long timeEnd = System.currentTimeMillis();
            long timeAll = timeEnd - timeStart;
            System.out.println("Tổng thời gian random gen data: " + timeAll / 60000 + " phút");

            lineReader.close();

            connection.close();

        } catch (IOException ex) {
            System.err.println(ex);
        } catch (SQLException ex) {
            ex.printStackTrace();

            try {
                connection.rollback();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }


    }
}
