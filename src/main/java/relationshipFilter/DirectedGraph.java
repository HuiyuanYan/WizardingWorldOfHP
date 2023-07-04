package relationshipFilter;

import java.util.*;
import java.text.DecimalFormat;
public class DirectedGraph {
    private Map<String,Map<String, Double>> adjacencyList;

    public DirectedGraph()
    {
        adjacencyList = new HashMap<>();
    }

    // 拷贝构造函数
    public DirectedGraph(DirectedGraph otherGraph) {
        adjacencyList = new HashMap<>();
        // 复制邻接表中的数据
        for (String node : otherGraph.adjacencyList.keySet()) {
            Map<String, Double> neighbors = otherGraph.adjacencyList.get(node);
            adjacencyList.put(node, new HashMap<>(neighbors));
        }
    }

    public void addNode(String node){
        if (!adjacencyList.containsKey(node)) {
            adjacencyList.put(node, new HashMap<>());
        }
    }

    public void addEdge(String source, String destination, double weight) {
        addNode(source);
        addNode(destination);
        Map<String, Double> neighbors = adjacencyList.get(source);
        neighbors.put(destination, weight);
    }

    public Set<String> getNodes() {
        return adjacencyList.keySet();
    }

    public double getEdgeWeight(String node1, String node2) {
        if (adjacencyList.containsKey(node1) && adjacencyList.get(node1).containsKey(node2)) {
            return adjacencyList.get(node1).get(node2);
        } else {
            return 0.0;
        }
    }

    public void removeEdge(String source, String destination) {
        if (adjacencyList.containsKey(source)) {
            Map<String, Double> neighbors = adjacencyList.get(source);

            neighbors.remove(destination);
        }
    }

    public Map<String, Double> getNeighbors(String node) {
        return adjacencyList.getOrDefault(node, new HashMap<>());
    }

    public boolean hasNode(String node) {
        return adjacencyList.containsKey(node);
    }

    /**
     * 归一化有向边权重
     */


    public void normalizeWeights() {
        // 遍历每个节点
        for (String node : adjacencyList.keySet()) {
            double weightSum = 0.0;

            // 计算该节点的边权重和
            Map<String, Double> neighbors = adjacencyList.get(node);
            for (double weight : neighbors.values()) {
                weightSum += weight;
            }

            // 更新每条边的归一化权重
            for (String neighbor : neighbors.keySet()) {
                double weight = neighbors.get(neighbor);
                double normalizedWeight = weight / weightSum;

                // 格式化归一化权重为五位小数
                DecimalFormat df = new DecimalFormat("#.#####");
                normalizedWeight = Double.parseDouble(df.format(normalizedWeight));

                neighbors.put(neighbor, normalizedWeight);
            }
        }
    }



    @Override
    public String toString() {
        List<String> sortedNodes = new ArrayList<>(adjacencyList.keySet());
        Collections.sort(sortedNodes);

        StringBuilder sb = new StringBuilder();
        for (String node : sortedNodes) {
            sb.append(node).append("\t[");
            Map<String, Double> neighbors = adjacencyList.get(node);
            List<String> neighborList = new ArrayList<>(neighbors.keySet());
            Collections.sort(neighborList);
            for (String neighbor : neighborList) {
                double weight = neighbors.get(neighbor);
                sb.append(neighbor).append("@").append(weight).append("|");
            }
            if (!neighborList.isEmpty()) {
                sb.deleteCharAt(sb.length() - 1);
            }
            sb.append("]").append(System.lineSeparator());
        }
        return sb.toString();
    }
}
