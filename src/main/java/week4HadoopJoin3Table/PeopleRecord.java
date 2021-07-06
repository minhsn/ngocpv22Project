package week4HadoopJoin3Table;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class PeopleRecord implements Writable {

    public Text id = new Text();
    public Text firstname = new Text();
    public Text lastname = new Text();
    public Text email = new Text();
    public Text city = new Text();
    public Text FieldName = new Text();
    public Text Score = new Text();

    public PeopleRecord(){}

    public PeopleRecord(String id,String firstname, String lastname, String email, String city, String FieldName, String Score){
        this.id.set(id);
        this.firstname.set(firstname);
        this.lastname.set(lastname);
        this.email.set(email);
        this.city.set(city);
        this.FieldName.set(FieldName);
        this.Score.set(Score);
    }

    public void write(DataOutput out) throws IOException {
        this.id.write(out);
        this.firstname.write(out);
        this.lastname.write(out);
        this.email.write(out);
        this.city.write(out);
        this.FieldName.write(out);
        this.Score.write(out);
    }

    public void readFields(DataInput in) throws IOException {
        this.id.readFields(in);
        this.firstname.readFields(in);
        this.lastname.readFields(in);
        this.email.readFields(in);
        this.city.readFields(in);
        this.FieldName.readFields(in);
        this.Score.readFields(in);
    }
}
