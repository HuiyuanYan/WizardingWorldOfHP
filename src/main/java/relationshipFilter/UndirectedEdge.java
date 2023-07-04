package relationshipFilter;
import java.util.*;
public class UndirectedEdge {
    private String node1;
    private String node2;
    private double weight;

    public UndirectedEdge(String node1, String node2, double weight) {
        this.node1 = node1;
        this.node2 = node2;
        this.weight = weight;
    }

    public String getNode1() {
        return node1;
    }

    public String getNode2() {
        return node2;
    }

    public double getWeight() {
        return weight;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        UndirectedEdge other = (UndirectedEdge) obj;
        return( (node1.equals(other.node1) && node2.equals(other.node2))  ||
                (node1.equals(other.node2) && node2.equals(other.node1)));
    }

    @Override
    public int hashCode() {
        return Objects.hash(node1, node2) + Objects.hash(node2, node1);
    }
}
