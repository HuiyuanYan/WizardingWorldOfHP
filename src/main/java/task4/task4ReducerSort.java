package task4;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
public class task4ReducerSort extends Reducer<DoubleWritable, Text, Text, DoubleWritable>{
    Text k = new Text();
    @Override
    protected void reduce(DoubleWritable key, Iterable<Text> values,Context context)
            throws IOException, InterruptedException {
        for(Text value : values){
            k.set(value.toString());
            context.write(k, new DoubleWritable(-key.get()));
        }
    }
}