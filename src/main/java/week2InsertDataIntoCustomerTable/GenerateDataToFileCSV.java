package week2InsertDataIntoCustomerTable;

import com.github.javafaker.Faker;
import com.opencsv.CSVWriter;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Locale;

public class GenerateDataToFileCSV {
    public static void main(String[] args) {
        long start = System.currentTimeMillis();
        writeDataLineByLine("C:\\CustomProject\\ngocpv22Project\\ngocpv22MasterDevSeason3\\src\\main\\java\\week2InsertDataIntoCustomerTable\\Data.csv");
        long endAll = System.currentTimeMillis();
        long timeAll = endAll - start;
        System.out.println("Tổng thời gian random gen data: " + timeAll/60000 + " phút");
    }
    public static void writeDataLineByLine(String filePath)
    {
        File file = new File(filePath);
        try {
            // create FileWriter object with file as parameter
            FileWriter outputfile = new FileWriter(file);

            // create CSVWriter object filewriter object as parameter
            CSVWriter writer = new CSVWriter(outputfile);

            // adding header to csv
            String[] header = {"pkg_order", "shop_code", "customer_tel", "customer_tel_normalize", "fullname", "pkg_created", "pkg_modified", "package_status_id", "customer_province_id", "customer_district_id", "customer_ward_id", "created", "modified", "is_cancel", "ightk_user_id" };
            writer.writeNext(header);


            Faker faker = new Faker(new Locale("vi"));
            // Bắt đầu quá trình generate dữ liệu
            int count = 0;
            while(count < 5000000){
                count ++;
                String[] dataLine = { String.valueOf(count) , "shop_" + faker.number().digits(3), faker.phoneNumber().phoneNumber() , faker.phoneNumber().cellPhone(), faker.name().fullName(), "2021-06-14 21:22:59", "2021-06-14 21:22:59", faker.number().digits(2), faker.number().digits(1), faker.number().digits(2), faker.number().digits(1), "2000-04-11 11:58:44", "2000-04-11 11:58:44", "1", faker.number().digits(2) };
                writer.writeNext(dataLine);
            }
            // closing writer connection
            writer.close();
        }
        catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
}
