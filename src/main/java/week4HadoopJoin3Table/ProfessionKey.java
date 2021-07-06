package week4HadoopJoin3Table;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class ProfessionKey implements WritableComparable<ProfessionKey> {
    public static final IntWritable SALARY_RECORD = new IntWritable(0);
    public static final IntWritable PEOPLE_RECORD = new IntWritable(1);

    //public IntWritable id = new IntWritable();
    public Text profession = new Text();
    public IntWritable recordType = new IntWritable(); //0 = Salary record, 1 = People record

    public ProfessionKey(){}

    public ProfessionKey(String profession, IntWritable recordType) {
        this.profession.set(profession);
        this.recordType = recordType;
    }
    public boolean equals (ProfessionKey other) {
        return this.profession.equals(other.profession) && this.recordType.equals(other.recordType);
    }
    // the same ProductId to arrive at the same reducer
    public int hashCode() {
        return this.profession.hashCode();
    }

    @Override
    public int compareTo(ProfessionKey other) {
        if (this.profession.equals(other.profession )) {
            return this.recordType.compareTo(other.recordType);
        } else {
            return this.profession.compareTo(other.profession);
        }
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        this.profession.write(dataOutput);
        this.recordType.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.profession.readFields(dataInput);
        this.recordType.readFields(dataInput);
    }
}
