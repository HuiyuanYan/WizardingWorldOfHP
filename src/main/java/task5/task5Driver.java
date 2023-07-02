package task5;

import org.apache.commons.math3.linear.ConjugateGradient;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.*;
import java.util.*;

public class task5Driver {


    /**
     * @description 读取task3的输出，建立无向图，运行K-core算法，获取必要信息
     * @param inputfilePath task3输出的文件路径
     * @param outputGraphInfoPath 图信息输出文件路径
     * @param outputLabelInfoPath 标签信息输出文件路径
     * @param fileSystem 文件系统
     * @return void
     * @author Yan
     * @date 2023/6/30 上午5:56
     */
    private static void readGraph(String inputfilePath,String outputGraphInfoPath,String outputLabelInfoPath,FileSystem fileSystem)
    {
        Map<String,String>neighborInfo = new HashMap<>();


        undirectedGraph graph = new undirectedGraph();
        try{
            BufferedReader reader = new BufferedReader(new InputStreamReader(fileSystem.open(new Path(inputfilePath))));
            String line;
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split("\t");

                String node = parts[0];
                //去除两边方括号
                String neighbors = parts[1].substring(1,parts[1].length() - 1);

                //添加node:["neighbor1,weight1|...|neighborN,weightN"]到neighborInfo
                neighborInfo.put(node,neighbors);

                //向图中添加节点和边（不需要添加权重）
                graph.addNode(node);

                String[] neighborWeightPairs = neighbors.split("\\|");

                Set<String> addedNeighbors = new HashSet<>();
                for (String neighbor : neighborWeightPairs) {
                    String neighborNode = neighbor.split("@")[0];
                    if (!addedNeighbors.contains(neighborNode)) {
                        graph.addEdge(node, neighborNode);
                        addedNeighbors.add(neighborNode);
                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        //然后运行K-Core算法，这里k值取平均度数（向下取整）

        int K = graph.getAverageDegree();

        //获取各个节点k值
        System.out.println("Running K-Core.");
        Map<String,Integer> nodeKValue = graph.getKValueOfNodes(K);
        System.out.println("K-Core finished.");
        //获取各个节点归一化度
        System.out.println("Running NormalizedDegree.");
        Map<String,Double> nodeNormalizedDegree = graph.getNormalizedDegreeValues();
        System.out.println("NormalizedDegree finished.");
        //计算各个节点的综合影响值 IN[i] = k[i]+NDegree[i];
        Map<String,Double> nodeInfluence = new HashMap<>();
        for(String node : nodeKValue.keySet())
        {
            nodeInfluence.put(node,nodeKValue.get(node)+nodeNormalizedDegree.get(node));
        }

        //将图信息输出到指定文件
        try {

            BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(fileSystem.create( new Path(outputGraphInfoPath), true)));

            // 遍历第一个 Map，输出每一行的数据
            for (Map.Entry<String, String> entry : neighborInfo.entrySet()) {
                String node = entry.getKey();
                String val1 = entry.getValue();
                Double val2 = nodeInfluence.getOrDefault(node, 0.0);

                // 将数据写入文件
                writer.write(node + "#" + val1+ "#" + val2  + "\n");
            }

            writer.close();
            System.out.println("数据已成功写入文件：" + outputGraphInfoPath);
        } catch (IOException e) {
            e.printStackTrace();
        }

        try {
            BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(fileSystem.create( new Path(outputLabelInfoPath), true)));
            //为核心层节点编号，并标识边缘层节点
            //找到最大K和最小K
            int k_min=Integer.MAX_VALUE,k_max=K;
            for(Integer k : nodeKValue.values())
            {
                if(k<k_min)
                {
                    k_min = k;
                }
            }

            int label_idx = 1;//label下标
            for(String node :nodeKValue.keySet())
            {

                int kValue = nodeKValue.get(node);
                //边缘层
                if(kValue==k_min)
                {
                    writer.write(node + "#Y#N\n");//表示该节点是边缘节点，且没有标签
                }
                else if(kValue == k_max)
                {
                    writer.write(node + "#N"+"#"+node+'\n');
                    label_idx += 1;
                }
                else{//其余的节点，非边缘层节点，也没有label
                    writer.write(node+"#N#N\n");
                }
            }
            writer.close();
            System.out.println("数据已成功写入文件：" + outputLabelInfoPath);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * @description 将图的信息读入到全局配置中
     * @param graphInfofilePath: 图信息文件路径
     * @param conf: 配置
     * @return void
     * @author Yan
     * @date 2023/6/30 下午8:14
     */

    private static void setConf(String graphInfofilePath, Configuration conf)
    {

        try (BufferedReader reader = new BufferedReader(new InputStreamReader(FileSystem.get(conf).open(new Path(graphInfofilePath))));) {
            String line;
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split("#");
                conf.setStrings(parts[0],parts[1],parts[2]);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }



    public static void main(String[] args) throws Exception {

        //task5配置
        Configuration conf = new Configuration();
        GenericOptionsParser optionParser = new GenericOptionsParser(conf, args);
        String[] remainingArgs = optionParser.getRemainingArgs();
        if ((remainingArgs.length != 2)) {
            System.err.println("Usage: HPTask5 <task3_output> <output>");
            System.exit(2);
        }
        //设置信息文件路径
        String graphInfoPath = "./graph_info.txt";

        int iterationCount = 0;
        String labelInfoPath = "./label_info" + iterationCount +".txt";
        conf.setStrings("preLabelInfoPath",labelInfoPath);//设置文件路径
        //解析图信息，运行K核算法，并设置初始Label，将信息输出到相应文件中
        readGraph(remainingArgs[0],graphInfoPath,labelInfoPath,FileSystem.get(conf));

        //将图信息配置到conf中
        setConf(graphInfoPath,conf);


        while(true)
        {
            iterationCount += 1;//迭代次数加一
            String newLabelInfoPath = "./label_info"+iterationCount;//新的label_info文件路径

            //用于标识当前处理是否为处理边缘层节点
            conf.setBoolean("handleEdge",false);


            Job job = Job.getInstance(conf, "HPTask5_Iter"+iterationCount);
            job.setJarByClass(task5Driver.class);
            job.setMapperClass(task5Mapper.class);
            job.setReducerClass(task5Reducer.class);
            //设置map输出的key类型
            job.setMapOutputKeyClass(Text.class);
            //设置map输出的value类型
            job.setMapOutputValueClass(NullWritable.class);

            //设置输出的key类型
            job.setOutputKeyClass(Text.class);
            //设置输出的value类型
            job.setOutputValueClass(NullWritable.class);

            conf.setStrings("preLabelInfoPath",labelInfoPath);//设置文件路径

            //输入文件为上一次迭代的输出文件
            FileInputFormat.addInputPath(job, new Path(labelInfoPath));
            FileOutputFormat.setOutputPath(job, new Path(newLabelInfoPath));

            System.out.println("Running task5 ,IterationCount = "+iterationCount);
            job.waitForCompletion(true);

            //删除上一次迭代输出
            //FileSystem.get(conf).delete(new Path(labelInfoPath),true);

            //更新输出文件路径
            labelInfoPath = newLabelInfoPath;

            //检查是否有标签被更新
            Counter updateLabel_counter = job.getCounters().findCounter(task5Mapper.CountersEnum.UPDATE_LABEL_NUM);
            if(updateLabel_counter.getValue()<=0)//如果没有标签被更新，退出循环
                break;
        }
        System.out.println("Finish Iteration.");
        //接下来更新边缘层节点
        conf.setBoolean("handleEdge",true);
        //将labelInfo文件设置为缓存文件
        Job job = Job.getInstance(conf, "HPTask5_Iter"+iterationCount);
        job.setJarByClass(task5Driver.class);
        job.setMapperClass(task5Mapper.class);
        job.setReducerClass(task5Reducer.class);
        //设置map输出的key类型
        job.setMapOutputKeyClass(Text.class);
        //设置map输出的value类型
        job.setMapOutputValueClass(NullWritable.class);

        //设置输出的key类型
        job.setOutputKeyClass(Text.class);
        //设置输出的value类型
        job.setOutputValueClass(NullWritable.class);

        conf.setStrings("preLabelInfoPath",labelInfoPath);//设置文件路径

        //输入文件为上一次迭代的输出文件
        FileInputFormat.addInputPath(job, new Path(labelInfoPath));

        //设置输出路径
        FileOutputFormat.setOutputPath(job, new Path(remainingArgs[1]));

        job.waitForCompletion(true);

        //删除上一次迭代输出
        //FileSystem.get(conf).delete(new Path(labelInfoPath),true);
        System.exit(0);
    }
}
