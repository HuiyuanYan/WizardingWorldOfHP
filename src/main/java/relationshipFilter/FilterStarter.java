package relationshipFilter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.*;
import java.util.*;
import java.util.stream.Collectors;

public class FilterStarter {

    private static UndirectedGraph graph2;

    private static DirectedGraph readGraph1(String filePath, FileSystem fileSystem)
    {
        DirectedGraph graph = new DirectedGraph();
        try {
            FileStatus[] fileStatuses = fileSystem.listStatus(new Path(filePath));
            for(FileStatus fileStatus :fileStatuses){
            String line;
            BufferedReader reader = new BufferedReader(new InputStreamReader(fileSystem.open(fileStatus.getPath())));
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split("\t");
                String node = parts[0];

                //去除两边方括号
                String neighborWeightStr = parts[1].substring(1, parts[1].length() - 1);

                graph.addNode(node);

                String[] neighborWeightPairs = neighborWeightStr.split("\\|");

                for(String s : neighborWeightPairs)
                {
                    String [] strings = s.split("@");
                    graph.addEdge(node,strings[0],Double.parseDouble(strings[1]));
                }

            }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return graph;
    }

    private static UndirectedGraph convertToUndirectedGraph(DirectedGraph directedGraph)
    {
        UndirectedGraph graph = new UndirectedGraph();

        for(String node : directedGraph.getNodes())
        {
            graph.addNode(node);
            Map<String, Double> neighbors = directedGraph.getNeighbors(node);
            for(Map.Entry<String,Double>entry:neighbors.entrySet())
            {
                String destination = entry.getKey();
                graph.addEdge(node,destination,0.0);//先把权重设置为0
            }
        }

        //统计图中最大度数
        int maxDegree = graph.getMaxDegree();

        //设置无向图边的新权重
        for(String node : graph.getNodes())
        {
            Map<String, Double> neighbors = graph.getNeighbors(node);
            for(String neighbor : neighbors.keySet())
            {
                //计算有向图的加权权重
                Double newWeight = directedGraph.getEdgeWeight(node,neighbor)*(graph.getDegree(node)/(double)maxDegree)
                        + directedGraph.getEdgeWeight(neighbor,node)*(graph.getDegree(neighbor)/(double)maxDegree);
                //设置无向图对应边的新权重
                graph.setEdgeWeight(node,neighbor,newWeight);
            }
        }

        return graph;
    }

    private static DirectedGraph filterEdge(Double alpha, DirectedGraph directedGraph,List<UndirectedEdge>edges)
    {
        //删边规则：删去每个节点较小的占比alpha的边，如果其中有边属于最小生成树的边，则不删除
        DirectedGraph graph = new DirectedGraph(directedGraph);

        //存储要删除的边
        List<MyPair<String,String>> edgesToRemove = new ArrayList<>();

        for(String node : directedGraph.getNodes())
        {
            Map<String,Double> neighbors = directedGraph.getNeighbors(node);
            List<Map.Entry<String, Double>> sortedList = neighbors.entrySet()
                    .stream()
                    .sorted(Map.Entry.comparingByValue())
                    .collect(Collectors.toList());
            int deleteEdgeNum = (int)Math.floor(neighbors.size()*alpha);
            //删除权重较小的边，把要保留的边加入到新有向图中
            for(int i = 0;i<sortedList.size();i++)
            {
                Map.Entry<String,Double> entry = sortedList.get(i);
                String neighbor = entry.getKey();
                Double weight = entry.getValue();
                if(i<deleteEdgeNum && !edges.contains(new UndirectedEdge(node,neighbor,weight)))
                {
                    //删除权重较小且不在生成树的边
                        edgesToRemove.add(new MyPair<String,String>(node,neighbor));
                }
            }
        }
        //删除边，要双向删除
        for(MyPair<String,String>myPair:edgesToRemove)
        {
            String node1 = myPair.getKey();
            String node2 = myPair.getValue();
            graph.removeEdge(node1,node2);
            graph.removeEdge(node2,node1);
        }

        //归一化有向边权重
        graph.normalizeWeights();
        return graph;
    }

    public static void main(String[] args) throws IOException {
        Configuration conf = new Configuration();
        GenericOptionsParser optionParser = new GenericOptionsParser(conf, args);
        String[] remainingArgs = optionParser.getRemainingArgs();
        if ((remainingArgs.length != 2)) {
            System.err.println("Usage: HPRelationshipFilter <input_dir> <out_dir>");
            System.exit(2);
        }
        FileSystem fileSystem = FileSystem.get(conf);

        DirectedGraph graph1 = readGraph1(remainingArgs[0],FileSystem.get(conf));
        UndirectedGraph graph2 = convertToUndirectedGraph(graph1);
        UndirectedGraph minimumSpanningTree = graph2.getMinimumSpanningTree();
        DirectedGraph newGraph = filterEdge(0.8,graph1,minimumSpanningTree.getEdges());

        //将结果写入到指定文件夹下
        try {
            // 创建 FileSystem 对象
            FileSystem fs = FileSystem.get(conf);

            // 创建输出流
            OutputStream outputStream = fs.create(new Path(remainingArgs[1]));
            BufferedOutputStream bufferedOutputStream = new BufferedOutputStream(outputStream);

            // 将数据结构字符串写入输出流
            bufferedOutputStream.write(newGraph.toString().getBytes("UTF-8"));

            // 关闭流
            bufferedOutputStream.close();
            fs.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
