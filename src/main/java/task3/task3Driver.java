package task3;

import task2.task2Driver.StringPair;
import task2.task2Partitioner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
public class task3Driver {
    public static void main(String[] args) throws Exception {

        //task2配置
        Configuration conf = new Configuration();
        GenericOptionsParser optionParser = new GenericOptionsParser(conf, args);
        String[] remainingArgs = optionParser.getRemainingArgs();
        if ((remainingArgs.length != 2)) {
            System.err.println("Usage: HPTask3 <in_path> <out_path>");
            System.exit(2);
        }

        Job job = Job.getInstance(conf, "HPTask3");
        job.setJarByClass(task3Driver.class);
        job.setMapperClass(task3Mapper.class);
        job.setReducerClass(task3Reducer.class);
        job.setPartitionerClass(task2Partitioner.class);

        //设置map输出的key类型
        job.setMapOutputKeyClass(StringPair.class);
        //设置map输出的value类型
        job.setMapOutputValueClass(IntWritable.class);
        //设置输出的key类型
        job.setOutputKeyClass(Text.class);
        //设置输出的value类型
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(remainingArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(remainingArgs[1]));
        job.waitForCompletion(true);
    }
}
