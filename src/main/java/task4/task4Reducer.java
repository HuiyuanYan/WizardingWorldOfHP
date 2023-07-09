package task4;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
/*
Reduce阶段主要完成以下任务
输入：<源网页：源网页的权值，链接网页的当前权值>此处为<角色名字：角色的过去权值，角色新的权值>
输出：<源网页，源网页的新权值>此处为<角色名字，角色的新pagerank>
1）	map阶段发送过来的 “链接网页的当前权值”进行累加 得到当前源网页的一个“暂时的”新权值
2）	因为有一些特殊情况，所以还要对此权值进行平滑处理（公式见上述说明），处理后便得到最终的新权值。
3）在输出之前，要比较源网页的新权值与权值之间的差异，若其差异小于某阈值，则可视为其已经得到最终结果，自定义计数器Counter+1，当自定义计数器的值等于源网页的总数时迭代结束。
*/
public class task4Reducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable>{
    Text k = new Text();
    DoubleWritable v = new DoubleWritable();
    Counter counter = null;

    //判断该网站的新、老权值是不是小于阈值
    public static boolean isChanged(double oldWeight, double newWeight){
        if(Math.abs(oldWeight-newWeight) < 0.001)
            return true;
        return false;
    }

    @Override
    protected void reduce(Text key, Iterable<DoubleWritable> values,Context context)
            throws IOException, InterruptedException {
        double tempWeight = 0;
        String []it=key.toString().split(":");
        double oldWeight = Double.parseDouble(it[1]);
        it[0]+='|';
        k.set(it[0]);
        for(DoubleWritable value : values){
            tempWeight += value.get();
        }
        //获得在configuration中保存的平滑因子以及源网页总数
        double a = Double.parseDouble(context.getConfiguration().get("smooth"));
        double e = 1.0/Integer.parseInt(context.getConfiguration().get("webNum"));
        //按公式得出新权值
        double newWeight = a*tempWeight + (1-a)*e;

        boolean flag = isChanged(oldWeight,newWeight);

        //若当前网页的新、老权值不变，则当前网页的权值收敛，counter+1
        if(flag){
            counter = context.getCounter("myCounter", "constringency");
            counter.increment(1L);
        }

        v.set(newWeight);
        context.write(k,v);
    }
}
