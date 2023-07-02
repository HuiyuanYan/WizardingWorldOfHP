package task3;

import task2.task2Driver.StringPair;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;

public class task3Reducer
        extends Reducer<StringPair, IntWritable, Text, Text> {
    String first_name="";
    String secon_name="";
    int cnt=0;
    int sum=0;
    String prev="";
    Map<String,IntWritable> posting_map=new HashMap<>();

    @Override
    public void reduce(StringPair key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        first_name=key.get_first_name();
        //calculate
        if(!prev.equals("")&&!prev.equals(first_name)){
            StringBuilder out= new StringBuilder();
            out.append("[");
            float res;
            for (Map.Entry<String, IntWritable> entry : posting_map.entrySet()) {
                out.append(entry.getKey());
                res= (float) entry.getValue().get() /sum;
                out.append("@").append(String.format("%.5f", res)).append("|");
            }
            out.deleteCharAt(out.length()-1);
            out.append("]");
            context.write(new Text(prev),new Text(out.toString()));
            posting_map=new HashMap<>();
            sum=0;
        }
        for(IntWritable value:values){
            sum+=value.get();
            cnt=value.get();
        }
        posting_map.put(key.get_second_name(),new IntWritable(cnt));
        prev=first_name;
    }

    public void cleanup(Context context)
            throws IOException, InterruptedException {
        StringBuilder out= new StringBuilder();
        out.append("[");
        float res;
        for (Map.Entry<String, IntWritable> entry : posting_map.entrySet()) {
            out.append(entry.getKey());
            res= (float) entry.getValue().get() /sum;
            out.append("@").append(String.format("%.5f", res)).append("|");
        }
        out.deleteCharAt(out.length()-1);
        out.append("]");
        context.write(new Text(prev),new Text(out.toString()));
    }
}

