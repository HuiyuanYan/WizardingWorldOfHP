package task2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class task2Reducer
        extends Reducer<task2Driver.StringPair, IntWritable, Text, IntWritable> {
    public void reduce(task2Driver.StringPair key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        int sum=0;
        for(IntWritable value:values){
            sum+=value.get();
        }
        context.write(new Text("<"+key.get_first_name()+","+key.get_second_name()+">"),new IntWritable(sum));
    }
}
