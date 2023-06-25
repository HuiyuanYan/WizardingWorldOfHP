package task1;

import org.apache.commons.lang3.ObjectUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

import java.net.URI;
import java.util.*;

public class task1Mapper
        extends Mapper<Object, Text, Text, NullWritable> {

    //人名字典，key为识别的词素，val为该词素对应人名，例如：Harry:Harry Potter;Harry Potter:Harry Potter
    private Hashtable<String,String> nameDict= new Hashtable();

    private Text text = new Text();



    /**
     * @description 从缓存文件中读取名字字典
     * @param uri: 缓存文件uri
     * @return void
     * @author Yan
     * @date 2023/6/23 上午5:58
     */
    private void readNameDict(URI uri)
    {
        try{
            Path patternsPath = new Path(uri.getPath());
            String patternsFileName = patternsPath.getName().toString();
            BufferedReader reader = new BufferedReader(new FileReader(
                    patternsFileName));
            String line;
            while ((line = reader.readLine()) != null) {
                // TODO: your code here
                //
                String[] strings = line.split(":");
                this.nameDict.put(strings[0],strings[1]);
            }
            reader.close();
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
    }


    @Override
    public void setup(Context context) throws IOException,
            InterruptedException {

        //System.err.println("Map Set Up");
        readNameDict(context.getCacheFiles()[0]);
        //System.out.println(nameDict);


    }


    @Override
    public void map(Object key, Text value, Context context
    ) throws IOException, InterruptedException {
        //取出段落中除连字符、撇号外的所有标点符号
        //System.out.println(value.toString());
        String line = value.toString().replaceAll("[\\p{Punct}&&[^\\-'’]]", "");
        try{
            //将单词按空格划分
            String[] strings = line.split("\\s+");
            //System.out.println(strings);
            String names = "";
            int nameNum = 0;//记录名字数量
            //用于匹配的临时字符串
            String prevLexeme = "";
            String nextLexeme = "";

            //寻找存在于名字字典中的最大匹配词素
            for(String s:strings)
            {
                nextLexeme += s;
                if(nameDict.containsKey(nextLexeme))
                {
                    prevLexeme = nextLexeme;
                }
                else{

                    if((prevLexeme!="")&&nameDict.containsKey(prevLexeme))
                    {
                        //此时找到了最大匹配词素，输出名字到names中
                        names += (nameDict.get(prevLexeme) + ',');
                        nameNum += 1;
                    }
                    prevLexeme = "";
                    nextLexeme = "";
                }
            }
            if(nameNum>1){//只有同时出现了两个及以上的人名才会被记录
                //去掉names的最后一个逗号字符
                names = names.substring(0, names.length()-1);
                text.set(names);
                context.write(text,NullWritable.get());
            }

        }catch (Exception e)
        {
            e.printStackTrace();
        }
    }
}
