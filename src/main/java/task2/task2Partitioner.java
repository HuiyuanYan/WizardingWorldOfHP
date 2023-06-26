package task2;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class task2Partitioner
        extends Partitioner<task2Driver.StringPair, IntWritable> {

    @Override
    public int getPartition(task2Driver.StringPair stringPair, IntWritable intWritable, int i) {
        return stringPair.get_first_name().hashCode()%i;
    }
}