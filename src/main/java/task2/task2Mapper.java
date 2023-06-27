package task2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class task2Mapper
        extends Mapper<Object, Text, task2Driver.StringPair, IntWritable> {
    private IntWritable _one=new IntWritable(1);
    @Override
    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {

        String[] names = value.toString().split(",");
        try{
            for(int i=0;i<names.length;i++){
                for(int j=i+1;j<names.length;j++){
                    if(!names[i].equals(names[j])) {
                        context.write(new task2Driver.StringPair(names[i], names[j]), _one);
                        context.write(new task2Driver.StringPair(names[j], names[i]), _one);
                    }
                }
            }
        }catch (Exception e) {
            e.printStackTrace();
        }
    }
}
