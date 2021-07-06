package week4Hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import  org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import javax.naming.Context;
import java.io.IOException;

public class JoinTable {

    public static class JoinGroupingComparator extends WritableComparator {
        public JoinGroupingComparator() {
            super (ProfessionKey.class, true);
        }

        @Override
        public int compare (WritableComparable a, WritableComparable b){
            ProfessionKey first = (ProfessionKey) a;
            ProfessionKey second = (ProfessionKey) b;

            return first.profession.compareTo(second.profession);
        }
    }

    public static class JoinSortingComparator extends WritableComparator {
        public JoinSortingComparator()
        {
            super (ProfessionKey.class, true);
        }

        @Override
        public int compare (WritableComparable a, WritableComparable b){
            ProfessionKey first = (ProfessionKey) a;
            ProfessionKey second = (ProfessionKey) b;

            return first.compareTo(second);
        }
    }

    public static class SalaryMapper extends Mapper<LongWritable,
            Text, ProfessionKey, JoinGenericWritable>{
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String[] recordFields = value.toString().split(",");
            //Condition for ignore header in csv file
            if(recordFields[0] != "profession"){
                String profession = recordFields[0];
                String salary = recordFields[1];

                ProfessionKey recordKey = new ProfessionKey(profession, ProfessionKey.SALARY_RECORD);
                SalaryRecord record = new SalaryRecord(salary);

                JoinGenericWritable genericRecord = new JoinGenericWritable(record);
                context.write(recordKey, genericRecord);
            }
        }
    }

    public static class PeopleMapper extends Mapper<LongWritable,
            Text, ProfessionKey, JoinGenericWritable> {
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String[] recordFields = value.toString().split(",");

            //Condition for ignore header in csv file
            if(recordFields[0] != "id"){
                String id = recordFields[0];
                String firstname = recordFields[1];
                String lastname = recordFields[2];
                String email = recordFields[3];
                String city = recordFields[4];
                String profession = recordFields[5];
                String FieldName = recordFields[6];

                ProfessionKey recordKey = new ProfessionKey(profession, ProfessionKey.PEOPLE_RECORD);
                PeopleRecord record = new PeopleRecord(id, firstname, lastname, email, city, FieldName);
                JoinGenericWritable genericRecord = new JoinGenericWritable(record);
                context.write(recordKey, genericRecord);
            }
        }
    }

    public static class JoinRecuder extends Reducer<ProfessionKey,
            JoinGenericWritable, NullWritable, Text> {

        //Adding only 1 header for all output
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            context.write(null, new Text("id,firstname,lastname,email,city,Field Name,profession,salary"));
        }

        public void reduce(ProfessionKey key, Iterable<JoinGenericWritable> values,
                           Context context) throws IOException, InterruptedException{
            StringBuilder output = new StringBuilder();
            String salary  = null;

            for (JoinGenericWritable v : values) {
                Writable record = v.get();
                if (key.recordType.equals(ProfessionKey.SALARY_RECORD)) {
                    SalaryRecord sRecord = (SalaryRecord) record;
                    salary = sRecord.salary.toString();
                }else if (key.recordType.equals(ProfessionKey.PEOPLE_RECORD)){
                    PeopleRecord pRecord = (PeopleRecord) record;
                    output.append(pRecord.id.toString()).append(",");
                    output.append(pRecord.firstname.toString()).append(",");
                    output.append(pRecord.lastname.toString()).append(",");
                    output.append(pRecord.email.toString()).append(",");
                    output.append(pRecord.city.toString()).append(",");
                    output.append(pRecord.FieldName.toString()).append(",");
                    output.append(key.profession.toString()).append(",");
                    output.append(salary).append("\n");
                }
            }
            //Remove line empty with each producer in output
            output.setLength(output.length() - 1);
            context.write(NullWritable.get(), new Text(output.toString()));
        }
    }

    public static void main(String[] allArgs) throws Exception{
        Configuration conf = new Configuration();

        String[] args = new GenericOptionsParser(conf, allArgs).getRemainingArgs();

        Job job = Job.getInstance(conf);
        job.setJarByClass(JoinTable.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setMapOutputKeyClass(ProfessionKey.class);
        job.setMapOutputValueClass(JoinGenericWritable.class);

        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, SalaryMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, PeopleMapper.class);

        job.setReducerClass(JoinRecuder.class);

        job.setSortComparatorClass(JoinSortingComparator.class);
        job.setGroupingComparatorClass(JoinGroupingComparator.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        FileOutputFormat.setOutputPath(job, new Path(allArgs[2]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
