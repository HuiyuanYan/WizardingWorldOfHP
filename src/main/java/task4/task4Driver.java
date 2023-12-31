package task4;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import java.net.URI;

public class task4Driver {
    public static void main(String[] args)throws Exception {
        //task4配置

        Configuration confInit = new Configuration();
        GenericOptionsParser optionParser = new GenericOptionsParser(confInit, args);
        String[] remainingArgs = optionParser.getRemainingArgs();
        if ((remainingArgs.length != 2)) {
            System.err.println("Usage: HPTask4 <cache_path> <in_path> <out_path>");
            System.exit(2);
        }
        //第一阶段
        String input = remainingArgs[0];
        String output = remainingArgs[1];
        String tmp_dir="./output4_tmp/";
        Job jobInit = Job.getInstance(confInit, "HPTask4Init");
        jobInit.setJarByClass(task4Driver.class);
        jobInit.setMapperClass(task4MapperInit.class);
        jobInit.setReducerClass(task4ReducerInit.class);

        jobInit.setMapOutputKeyClass(Text.class);
        jobInit.setMapOutputValueClass(Text.class);
        jobInit.setOutputKeyClass(Text.class);
        jobInit.setOutputValueClass(DoubleWritable.class);

        FileInputFormat.addInputPaths(jobInit, input);

        int index = 0;
        String tmp_output  = tmp_dir + "task4_"+index;
        index += 1;
        FileOutputFormat.setOutputPath(jobInit, new Path(tmp_output));

        jobInit.waitForCompletion(true);

        String weightPath = tmp_output;

        //拿到统计源网页个数的计数器 并源网页总数
        Counter counterInit = jobInit.getCounters().getGroup("myCounter").findCounter("webNum");
        Long webNum = counterInit.getValue();

        //第二部分，迭代执行

        while(true){
            Configuration conf = new Configuration();
            conf.set("smooth", 0.8+"");	//设置平滑因子
            conf.set("webNum",webNum+"");//设置源网页总数

            Job job = Job.getInstance(conf,"HPTask4Yes");
            job.setJarByClass(task4Driver.class);
            job.setMapperClass(task4Mapper.class);
            job.setReducerClass(task4Reducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(DoubleWritable.class);

            FileInputFormat.setInputPaths(job, new Path(input));

            tmp_output = tmp_dir +"task4_"+index;
            FileOutputFormat.setOutputPath(job, new Path(tmp_output));

            weightPath=weightPath+"/part-r-00000";
            //weightPath = java.net.URLDecoder.decode(weightPath, "utf-8");
            //Thread.sleep(2500);
            job.addCacheFile(new Path(weightPath).toUri());
//            URI[] uris = job.getCacheFiles();
//            if(uris != null)
//                for(int i=0;i<uris.length;i++){
//                    System.out.println(uris[i].getPath().toString());
//                }

            weightPath = tmp_output;

            job.waitForCompletion(true);

            //通过计数器得知是否结束迭代

            Counter counter = job.getCounters().getGroup("myCounter").findCounter("constringency");
            Long constringency = counter.getValue();
            //当所有网页都收敛时，迭代结束
            if(webNum.equals(constringency)){
                break;
            }else{
                index++;
            }
        }

        //第三阶段
        Configuration confSort = new Configuration();
        Job jobSort = Job.getInstance(confSort,"HPTask4Sort");
        jobSort.setJarByClass(task4Driver.class);
        jobSort.setMapperClass(task4MapperSort.class);
        jobSort.setReducerClass(task4ReducerSort.class);

        jobSort.setMapOutputKeyClass(DoubleWritable.class);
        jobSort.setMapOutputValueClass(Text.class);
        jobSort.setOutputKeyClass(Text.class);
        jobSort.setOutputValueClass(NullWritable.class);

        FileInputFormat.setInputPaths(jobSort, new Path(tmp_dir +"task4_"+index));
        FileOutputFormat.setOutputPath(jobSort, new Path(output));

        jobSort.waitForCompletion(true);

    }

}
