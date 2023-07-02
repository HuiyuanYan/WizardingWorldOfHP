package task5;

import org.apache.commons.lang3.ObjectUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

import java.io.InputStreamReader;
import java.net.URI;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
public class task5Mapper
        extends Mapper<Object, Text, Text, NullWritable> {

    static enum CountersEnum { UPDATE_LABEL_NUM};//更新数量标签
    //节点-标签字典
    private Hashtable<String,String> labelInfo= new Hashtable();

    //节点-影响值字典
    private Hashtable<String,Double> nodeInfluence = new Hashtable<>();

    //节点-邻居|权重 字典
    private Hashtable<String, String> nodeNeighborWeight = new Hashtable<>();
    
    private Text text = new Text();
    

    private Configuration conf;

    private boolean handleEdge;
    /**
     * @description 从缓存文件中读取名字字典
     * @param labelInfoPath: 缓存文件路径
     * @param fileSystem :Hadoop文件系统
     * @return void
     * @author Yan
     * @date 2023/6/23 上午5:58
     */
    private void readNodeLabel(String labelInfoPath, FileSystem fileSystem)
    {

        try{
            Path patternsPath = new Path(labelInfoPath);

            FileStatus fileStatus = fileSystem.getFileStatus(patternsPath);
            if(fileStatus.isFile())
            {
                String line;
                BufferedReader reader = new BufferedReader(new InputStreamReader(fileSystem.open(fileStatus.getPath())));
                while ((line = reader.readLine()) != null) {
                    // TODO: your code here
                    String[] strings = line.split("#");
                    this.labelInfo.put(strings[0],strings[2]);
                }
                reader.close();
            }
            else if(fileStatus.isDirectory())
            {
                FileStatus[] fileStatuses = fileSystem.listStatus(patternsPath);
                for(FileStatus fileStatus1 : fileStatuses)
                {
                    BufferedReader reader = new BufferedReader(new InputStreamReader(fileSystem.open(fileStatus1.getPath())));
                    if(fileStatus1.isFile())
                    {
                        String line;
                        while ((line = reader.readLine()) != null) {
                            // TODO: your code here
                            String[] strings = line.split("#");
                            this.labelInfo.put(strings[0],strings[2]);
                        }
                    }
                    reader.close();

                }
            }



        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
    }

    private void readConf()
    {
        for(String node : this.labelInfo.keySet())
        {
            String[] strings = conf.getStrings(node);
            nodeNeighborWeight.put(node,strings[0]);
            nodeInfluence.put(node,Double.parseDouble(strings[1]));
        }
        handleEdge = conf.getBoolean("handleEdge",false);
    }
    @Override
    public void setup(Context context) throws IOException,
            InterruptedException {

        //读取配置
        conf = context.getConfiguration();
        System.err.println(conf.getStrings("preLabelInfoPath")[0]);
        readNodeLabel(conf.getStrings("preLabelInfoPath")[0],FileSystem.get(conf));
        readConf();

        //初始化Counter
        context.getCounter(CountersEnum.UPDATE_LABEL_NUM).setValue(0);
    }

    /**
     * @description 根据图信息以及原始标签信息更新label
     * @param node: 要更新的节点名
     * @param oldLabel: 旧标签
     * @return java.lang.String 新标签
     * @author Yan
     * @date 2023/7/1 上午1:13
     */
    private String updateLabel(String node,String isEdge,String oldLabel)
    {

        if((handleEdge && isEdge.equals("N")) || (!handleEdge && isEdge.equals("Y")))
        {
            //如果要求处理边缘节点，但节点是非边缘节点，或要求处理非边缘节点，但节点是边缘节点，不更新标签
            return oldLabel;
        }

        //相邻标签组对该节点的影响
        Hashtable<String,Double>labelInfluence = new Hashtable<>();

        //将自己的标签放入
        if(!oldLabel.equals("N"))
        {
            labelInfluence.put(oldLabel,nodeInfluence.get(node));
        }

        //读取该节点的邻居和权重信息
        String nodeInfo = nodeNeighborWeight.get(node);

        String[] neighborWeight = nodeInfo.split("\\|");
        for(String s : neighborWeight)
        {
            String[] pair = s.split("@");
            String neighborLabel = labelInfo.get(pair[0]);
            if(!neighborLabel.equals("N"))
            {
                double influence;
                if(labelInfluence.containsKey(neighborLabel)) {
                    influence = labelInfluence.get(neighborLabel) +
                            nodeInfluence.get(pair[0]) * Double.parseDouble(pair[1]);
                }
                else{
                    influence =  nodeInfluence.get(pair[0]) * Double.parseDouble(pair[1]);
                }
                labelInfluence.put(neighborLabel,influence);
            }
        }

        //将labelInfluence最大的那一个label作为新label
        if(labelInfluence.isEmpty())
        {//如果字典中没有值（其所有邻居标签为空，则返回原标签。
            return oldLabel;
        }
        else {
            //选取影响值最大的那个label作为新label
            String newLabel = null;
            double maxInfluence = Double.MIN_VALUE;

            for (Map.Entry<String, Double> entry : labelInfluence.entrySet()) {
                String key = entry.getKey();
                double value = entry.getValue();
                if (value > maxInfluence) {
                    newLabel = key;
                    maxInfluence = value;
                }
            }
            return newLabel;
        }

    }


    @Override
    public void map(Object key, Text value, Context context
    ) throws IOException, InterruptedException {
        String line = value.toString();
        try{
            //将单词按'#'划分
            String[] strings = line.split("#");//node,isEdge,oldLabel

            //用更新算法获取新标签
            String newLabel = updateLabel(strings[0],strings[1],strings[2]);
            String newLabelInfo = strings[0]+"#"+strings[1]+"#"+newLabel;
            //oldLabel != newLabel，发生了更新
            if(!strings[2].equals(newLabel))
            {
                context.getCounter(CountersEnum.UPDATE_LABEL_NUM).increment(1);
            }
            text.set(newLabelInfo);
            context.write(text,NullWritable.get());


        }catch (Exception e)
        {
            e.printStackTrace();
        }
    }
}
