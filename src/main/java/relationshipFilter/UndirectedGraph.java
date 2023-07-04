package relationshipFilter;
import java.util.*;

/**
 * 无向带权图
 */



public class UndirectedGraph {
    private Map<String, Map<String, Double>> adjacencyList;

    public UndirectedGraph() {
        adjacencyList = new HashMap<>();
    }

    public void addNode(String node) {
        if (!adjacencyList.containsKey(node)) {
            adjacencyList.put(node, new HashMap<>());
        }
    }

    public void addEdge(String node1, String node2, double weight) {
        addNode(node1);
        addNode(node2);
        adjacencyList.get(node1).put(node2, weight);
        adjacencyList.get(node2).put(node1, weight);
    }

    public Map<String, Double> getNeighbors(String node) {
        return adjacencyList.getOrDefault(node, new HashMap<>());
    }

    public int getMaxDegree() {
        int maxDegree = 0;
        for (String node : adjacencyList.keySet()) {
            int degree = adjacencyList.get(node).size();
            if (degree > maxDegree) {
                maxDegree = degree;
            }
        }
        return maxDegree;
    }

    public int getDegree(String node) {
        return adjacencyList.get(node).size();
    }

    public Set<String> getNodes() {
        return adjacencyList.keySet();
    }

    public void setEdgeWeight(String node1, String node2, double weight) {
        if (adjacencyList.containsKey(node1) && adjacencyList.containsKey(node2)) {
            adjacencyList.get(node1).put(node2, weight);
            adjacencyList.get(node2).put(node1, weight);
        }
    }

    public boolean hasNode(String node) {
        return adjacencyList.containsKey(node);
    }

    public List<UndirectedEdge> getEdges()
    {
        List<UndirectedEdge> edges = new ArrayList<>();
        for (String node1 : adjacencyList.keySet()) {
            Map<String, Double> neighbors = adjacencyList.get(node1);
            for (String node2 : neighbors.keySet()) {
                double weight = neighbors.get(node2);
                if (!edges.contains(new UndirectedEdge(node1, node2,weight))) {
                    edges.add(new UndirectedEdge(node1, node2, weight));
                }
            }
        }
        return edges;
    }

    public UndirectedGraph getMinimumSpanningTree()
    {
        UndirectedGraph minimumSpanningTree = new UndirectedGraph();

        //获取边集
        List<UndirectedEdge> edges = getEdges();

        // 按照边的权重降序进行排序
       // edges.sort(Comparator.comparingDouble(Edge::getWeight).reversed());
        edges.sort(Comparator.comparingDouble(UndirectedEdge::getWeight));
        //运行Kruskal算法
        List<String> nodes = new ArrayList<>(adjacencyList.keySet());

        //初始化并查集
        UnionFind unionFind = new UnionFind(nodes.size());

        //降序遍历边
        for(UndirectedEdge edge : edges)
        {
            String node1 = edge.getNode1();
            String node2 = edge.getNode2();
            double weight = edge.getWeight();

            int idx1 = nodes.indexOf((node1));
            int idx2 = nodes.indexOf((node2));
            //两节点在不同的连通分支
            if(unionFind.collapsingFind(idx1)!= unionFind.collapsingFind(idx2))
            {
                //合并其分支，同时向连通分支加入这条边
                unionFind.weightedUnion(idx1,idx2);
                minimumSpanningTree.addEdge(node1,node2,weight);
            }
        }
        return minimumSpanningTree;
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
