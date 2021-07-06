package week2Avro;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;

import java.io.File;
import java.io.IOException;

public class ProduceAndConsumeInAvroFile {
    public static Schema makeSchema() throws IOException
    {
        Schema.Parser parser = new Schema.Parser();
        Schema schema = parser.parse(new File("/home/ngocpham/Documents/Text Document/MasterDev/ngocpv22Project/src/main/java/week2Avro/userInfo.json"));
        return schema;
    }
    public static GenericData.Record makeObject(Schema schema, String username, int age, String phone, String street, String city, String country)
    {
        GenericData.Record record = new GenericData.Record(schema);
        record.put("username", username);
        record.put("age", age);
        record.put("phone", phone);
        GenericData.Record addr = new GenericData.Record(schema.getField("address").schema());
        addr.put("street", street);
        addr.put("city", city);
        addr.put("country", country);
        record.put("address", addr);

        return(record);
    }
    public static void writeFileAVRO(File file, Schema schema) throws IOException
    {
        GenericDatumWriter<GenericData.Record> datum = new GenericDatumWriter<GenericData.Record> (schema);
        DataFileWriter<GenericData.Record> writer = new DataFileWriter<GenericData.Record>(datum);

        writer.create(schema, file);

        writer.append(makeObject(schema, "Ngoc", 24, "012356", "Trần Huu Duc", "Ha Noi", "Viet Nam"));
        writer.append(makeObject(schema, "Betty", 25, "NJ", "Trần Huu Duc", "Ha Noi", "Viet Nam"));
        writer.append(makeObject(schema, "Carol", 26, "WA", "Trần Huu Duc", "Ha Noi", "Viet Nam"));
        writer.append(makeObject(schema, "Carol", 26, "WA", "Trần Huu Duc", "Ha Noi", "Viet Nam"));

        writer.close();
    }

    public static void readFileAVRO(File file) throws IOException
    {
        GenericDatumReader<GenericData.Record> datum = new GenericDatumReader<GenericData.Record>();
        DataFileReader<GenericData.Record> reader = new DataFileReader<GenericData.Record>(file, datum);

        GenericData.Record record = new GenericData.Record(reader.getSchema());

        while(reader.hasNext())
        {
            reader.next(record);
            System.out.println("Name: " + record.get("username") + ". Age: " + record.get("age") + ". Phone: " + record.get("phone")
                    + ". Address: " + record.get("address"));
        }
        reader.close();
    }
    public static void main(String[] args) {
        try
        {
            Schema schema = makeSchema();
            File file = new File("/home/ngocpham/Documents/Text Document/MasterDev/BaiTap2-AVRO/ProjectAvroJava/src/main/java/userInfo.avro");
            writeFileAVRO(file,schema);
            readFileAVRO(file);
        }
        catch(IOException e)
        {
            e.printStackTrace();
        }

    }
}
