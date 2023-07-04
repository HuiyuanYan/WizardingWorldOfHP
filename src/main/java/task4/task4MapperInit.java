package task4;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
//第一阶段首先统计人数n，并将初始的每个人的权值设置为 1/n
/*
 * 此时的map输出<"count",人名>，并且统计人数 将其赋值到counter中
 */

public class task4MapperInit extends Mapper<LongWritable, Text, Text, Text>{
    //同一输出的键为count 这样只需在reduce阶段遍历一遍集合即可得到网页总数
    Text k = new Text("count");
    Text v = new Text();
    Counter counter = null;
    @Override
    protected void map(LongWritable key, Text value,Context context)
            throws IOException, InterruptedException {
        // 计数器+1,这个计数器在reduce阶段拿不到，只是给最后的控制迭代的main方法用
        counter = context.getCounter("myCounter", "webNum");
        counter.increment(1L);
        String line = value.toString();
        String name = line.split("\\[")[0];
        v.set(name);
        context.write(k, v);

    }
}
