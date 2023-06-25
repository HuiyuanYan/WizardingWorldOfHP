package task1;

import org.apache.commons.math3.linear.ConjugateGradient;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.util.*;

public class task1Driver {

    public static void main(String[] args) throws Exception {

        //task1配置
        Configuration conf = new Configuration();
        GenericOptionsParser optionParser = new GenericOptionsParser(conf, args);
        String[] remainingArgs = optionParser.getRemainingArgs();
        if ((remainingArgs.length != 3)) {
            System.err.println("Usage: HPTask1 <nameList_path> <in_path> <out_path>");
            System.exit(2);
        }

        Job job = Job.getInstance(conf, "HPTask1");
        job.setJarByClass(task1Driver.class);
        job.setMapperClass(task1Mapper.class);
        job.setReducerClass(task1Reducer.class);

        //设置map输出的key类型
        job.setMapOutputKeyClass(Text.class);
        //设置map输出的value类型
        job.setMapOutputValueClass(NullWritable.class);
        //设置输出的key类型
        job.setOutputKeyClass(Text.class);
        //设置输出的value类型
        job.setOutputValueClass(NullWritable.class);


        //将人名字典添加到缓存文件中
        job.addCacheFile(new Path(remainingArgs[0]).toUri());
        FileInputFormat.addInputPath(job, new Path(remainingArgs[1]));
        FileOutputFormat.setOutputPath(job, new Path(remainingArgs[2]));
        job.waitForCompletion(true);
        System.exit(0);
    }
}
