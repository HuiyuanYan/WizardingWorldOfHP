package task5;

import javafx.beans.binding.MapBinding;

import java.util.*;
public class undirectedGraph {
    private Map<String,Set<String>>adjacencyMap;

    public undirectedGraph() {
        adjacencyMap = new HashMap<>();
    }
    public void addNode(String node)
    {
        adjacencyMap.putIfAbsent(node,new HashSet<>());
    }

    public void addEdge(String node1, String node2)
    {
        if(!adjacencyMap.containsKey(node1))
        {
            adjacencyMap.put(node1,new HashSet<>());
        }
        if(!adjacencyMap.containsKey(node2))
        {
            adjacencyMap.put(node2,new HashSet<>());
        }
        adjacencyMap.get(node1).add(node2);
        adjacencyMap.get(node2).add(node1);
    }

    public Set<String> getNeighbors(String node)
    {
        return adjacencyMap.getOrDefault(node, new HashSet<>());
    }

    public Set<String> getNodes() {
        return adjacencyMap.keySet();
    }

    public void printGraph() {
        for (Map.Entry<String, Set<String>> entry : adjacencyMap.entrySet()) {
            String node = entry.getKey();
            Set<String> neighbors = entry.getValue();

            System.out.print("Node: " + node + ", Neighbors: ");
            for (String neighbor : neighbors) {
                System.out.print(neighbor + " ");
            }
            System.out.println();
        }
    }



    /**
     * @description 根据K-core算法获取每个节点的k值
     * @param K: K-core参数
     * @return java.util.Map<java.lang.String,java.lang.Integer> 节点:k值对
     * @author Yan
     * @date 2023/6/30 上午5:24
     */

    public Map<String,Integer>getKValueOfNodes(int K)
    {
        System.out.println("K="+String.valueOf(K));
        Map<String, Set<String>> adjacencyMapCopy = new HashMap<>(adjacencyMap);
        Map<String,Integer> nodeKValueMap = new HashMap<>();

        //初始化所有节点的k值为K
        for(String node : adjacencyMapCopy.keySet())
        {
            nodeKValueMap.put(node,K);
        }

        //K-core算法，每次递归删除k(从1到K-1)度节点，并更新删除节点k值
        for(int k=1;k<K;k+=1)
        {
            if(adjacencyMapCopy.isEmpty())
                break;
            System.out.println("k="+String.valueOf(k));
            Set<String> nodesToRemove = new HashSet<>();
            //递归删除，直至删除后的图中再无k度节点
            boolean hasDegreeKNodes = true;
            while(hasDegreeKNodes)
            {
                hasDegreeKNodes = false;
                for (Map.Entry<String, Set<String>> entry : adjacencyMapCopy.entrySet()) {
                    String node = entry.getKey();
                    Set<String> neighbors = entry.getValue();

                    if (neighbors.size() <= k && !nodesToRemove.contains(node)) {
                        //更新被删除节点的k值
                        nodeKValueMap.put(node,k);
                        nodesToRemove.add(node);
                        hasDegreeKNodes = true;
                    }
                }
            }
            for (String node : nodesToRemove) {
                //删除节点
                for(String neighbor : adjacencyMapCopy.get(node))
                {
                    adjacencyMapCopy.get(neighbor).remove(node);
                }
                adjacencyMapCopy.remove(node);
            }
        }
        return nodeKValueMap;
    }

    /**
     * @description 获取节点及其归一化度值，归一化度为该节点的度除以图中最大度
     * @param :
     * @return java.util.Map<java.lang.String,java.lang.Double> 节点及归一化度的Map
     * @author Yan
     * @date 2023/6/30 上午5:51
     */

    public Map<String, Double> getNormalizedDegreeValues() {
        Map<String, Double> normalizedDegreeValues = new HashMap<>();

        int maxDegree = 0;
        for (Set<String> neighbors : adjacencyMap.values()) {
            int degree = neighbors.size();
            if (degree > maxDegree) {
                maxDegree = degree;
            }
        }

        for (Map.Entry<String, Set<String>> entry : adjacencyMap.entrySet()) {
            String node = entry.getKey();
            Set<String> neighbors = entry.getValue();

            double normalizedDegree = (double) neighbors.size() / maxDegree;
            normalizedDegree = Math.round(normalizedDegree * 10000.0) / 10000.0; // 保留四位小数

            normalizedDegreeValues.put(node, normalizedDegree);
        }

        return normalizedDegreeValues;
    }

    /**
     获取平均度数，下取整
     */
    public int getAverageDegree() {
        int totalDegree = 0;
        int numNodes = adjacencyMap.size();

        for (Set<String> neighbors : adjacencyMap.values()) {
            totalDegree += neighbors.size();
        }

        if (numNodes == 0) {
            return 0;
        }

        double averageDegree = (double) totalDegree / numNodes;
        return (int) Math.floor(averageDegree);
    }
}
