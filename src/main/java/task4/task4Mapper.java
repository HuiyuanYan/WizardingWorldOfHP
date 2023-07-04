package task4;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import java.io.IOException;
import java.util.Map;
import java.util.HashMap;
import java.io.*;
import java.net.URI;


//输入文件中的每一行数据 即是M矩阵的每一列数据
/*
 Map阶段的任务如下：
输入：<源网页：所有链接网页网页>
输出：<链接网页：上一轮该链接网页的权值，链接网页的当前的部分权值>
 1）首先拿到缓存到的每个网页的权值数据，并将其放到map集合中
 2）读取某个源网页链接到的所有网页数量 从而获得到源网页到每个链接网页的跳转概率,
 3）再通过缓存数据得到源网页的权值 ，并将其参与计算 得出链接网页的部分权值 （其总权值需要reduce端合并）
 */
public class task4Mapper extends
        Mapper<LongWritable, Text, Text, DoubleWritable> {
    Map<String, String> weightMap = new HashMap<String, String>();

    // 准备阶段将缓存进工作节点的概率分布表读出
    @Override
    protected void setup(Context context) throws IOException,
            InterruptedException {
        //拿到缓存文件数组
        //URI[] uris = context.getCacheFiles();
        //往缓存文件上兑一根输入流

//        BufferedReader br = new BufferedReader(new InputStreamReader(
//                new FileInputStream(uris[0].getPath().toString())));
        URI uri=context.getCacheFiles()[0];
        Path itpath=new Path(uri.getPath());
        String filename=itpath.getName().toString();
        BufferedReader br=new BufferedReader(new FileReader(filename));
        String line;

        while ((line = br.readLine()) != null) {
            String []field = line.split("\\|");
            weightMap.put(field[0].split("\t")[0], field[1]);
            System.out.println(field[0].split("\t")[0]+":"+field[1]);
        }
//        while (StringUtils.isNotEmpty(line = br.readLine())) {
//            String []field = line.split("\t");
//            weightMap.put(field[0], field[1]);
//        }
        br.close();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String line = value.toString();
        String[] resource_links = line.split("\\[");
        String resource = resource_links[0].split("\t")[0];
        String houmian=resource_links[1].substring(0,resource_links[1].length()-1);
        String[] links = houmian.split("\\|");
        boolean containRes = false;//记录是否存在链接到角色自身的链接

        int linkNum = links.length;	//源网页中所有链接网页的数量
        double sourceWeight;
        sourceWeight= Double.parseDouble(weightMap.get(resource));	//该角色当前权值
        //在源网页之中的所有链接网页的“当前”权值都一样
        //double partLinkWeight = (1.0 / linkNum) * sourceWeight;

        for (int i = 0; i < linkNum; i++) {
            String[]units=links[i].split(",");
            //当存在链接到角色自身的链接时 布尔值改为true;
            if (units[0].equals(resource))
                containRes = true;

            // 键为  <name：上一轮的pagerank,new pagerank
            //这样做的目的是为了新、老权值的变化，从而判断迭代是否完成
            double it=Double.parseDouble(units[1]);
            it*=sourceWeight;
            if(!weightMap.containsKey(units[0])){
                System.out.println(units[0]+"No found");
                System.out.println(weightMap.get(units[0])+"?");
                continue;
            }
            System.out.println(weightMap.get(units[0]));
            context.write(new Text(units[0] + ":" + weightMap.get(units[0])),
                    new DoubleWritable(it));
        }
        // 若网页的链接目标中没有本身，则输出<角色名字:pagerank,0.0> 以防丢失
//        String its=String.valueOf(sourceWeight);
//        if (!containRes)
//            context.write(
//                    new Text(resource + ":"+ its),
//                    new DoubleWritable(0.0));
    }
}
