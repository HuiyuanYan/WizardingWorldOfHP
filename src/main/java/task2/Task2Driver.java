package task2;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.*;

public class Task2Driver {
    public static class StringPair
            implements WritableComparable<StringPair> {
        private String _first_name;
        private String _second_name;
        public String get_first_name(){
            return _first_name;
        }
        public String get_second_name(){
            return _second_name;
        }
        StringPair(){
            _first_name="";
            _second_name="";
        }
        StringPair(String first_name,String second_name){
            _first_name=first_name;
            _second_name=second_name;
        }
        public void set(String first_name,String second_name){
            _first_name=first_name;
            _second_name=second_name;
        }
        @Override
        public void write(DataOutput out) throws IOException {
            out.writeUTF(_first_name);
            out.writeUTF(_second_name);
        }

        @Override
        public void readFields(DataInput dataInput) throws IOException {
            _first_name=dataInput.readUTF();
            _second_name=dataInput.readUTF();
        }


        /**
         * sorted by firstname
         * @param o the object to be compared.
         * @return
         */
        @Override
        public int compareTo(StringPair o) {
            //first decrease second decrease
            if(_first_name.compareTo(o._first_name)<0) return -1;
            else if(_first_name.compareTo(o._first_name)==0){
                return (_second_name.compareTo(o._second_name));
            }
            else return 1;
        }
    }
    public static void main(String[] args) throws Exception {

        //task2配置
        Configuration conf = new Configuration();
        GenericOptionsParser optionParser = new GenericOptionsParser(conf, args);
        String[] remainingArgs = optionParser.getRemainingArgs();
        if ((remainingArgs.length != 2)) {
            System.err.println("Usage: HPTask2 <in_path> <out_path>");
            System.exit(2);
        }

        Job job = Job.getInstance(conf, "HPTask2");
        job.setJarByClass(Task2Driver.class);
        job.setMapperClass(Task2Mapper.class);
        job.setReducerClass(Task2Reducer.class);
        job.setPartitionerClass(Task2Partitioner.class);

        //设置map输出的key类型
        job.setMapOutputKeyClass(StringPair.class);
        //设置map输出的value类型
        job.setMapOutputValueClass(IntWritable.class);
        //设置输出的key类型
        job.setOutputKeyClass(Text.class);
        //设置输出的value类型
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(remainingArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(remainingArgs[1]));
        job.waitForCompletion(true);
        System.exit(0);
    }

}
