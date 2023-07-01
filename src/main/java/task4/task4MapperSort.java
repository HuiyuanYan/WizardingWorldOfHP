package task4;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
//sort

public class task4MapperSort extends Mapper<LongWritable, Text, DoubleWritable, Text>{
    Text v = new Text();
    DoubleWritable k = new DoubleWritable();
    @Override
    protected void map(LongWritable key, Text value,Context context)
            throws IOException, InterruptedException {
        String line = value.toString();
        String webAddr_weight[] = line.split("\t");
        v.set( webAddr_weight[0]);
        //给权值取相反数，其目的是依权值的从大到小排序
        k.set(-Double.parseDouble(webAddr_weight[1]));
        context.write(k, v);
    }
}
