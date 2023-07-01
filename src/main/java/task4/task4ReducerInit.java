package task4;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/*
 此时的reduce阶段统计总数n 并且将 map阶段传过来的“值”作为“键”  1/n作为“值”  输出 格式为（人名，初始PAGERANK值）
 */
public class task4ReducerInit extends Reducer<Text, Text, Text, DoubleWritable>{
    Text k = new Text();
    DoubleWritable v = new DoubleWritable();
    @Override
    protected void reduce(Text key, Iterable<Text> values,Context context)
            throws IOException, InterruptedException {
        Long n = (long) 0;
        List<String> newValues = new ArrayList<String>();
        for(Text value : values){
            newValues.add(value.toString());
            n++;
        }

        for(String value : newValues){
            k.set(value.toString());
            v.set( 1.0/n);
            context.write(k, v);
        }
    }
}