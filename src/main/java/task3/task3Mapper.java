package task3;
import task2.task2Driver.StringPair;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class task3Mapper
        extends Mapper<Object, Text, StringPair, IntWritable> {
    @Override
    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {

        String line = value.toString();
        try{
            String first_name=line.substring(line.indexOf('<')+1,line.indexOf(','));
            String second_name=line.substring(line.indexOf(',')+1,line.indexOf('>'));
            IntWritable cnt=new IntWritable(Integer.parseInt(line.substring(line.indexOf('>')+1).trim()));
            context.write(new StringPair(first_name,second_name),cnt);
        }catch (Exception e) {
            e.printStackTrace();
        }
    }
}
