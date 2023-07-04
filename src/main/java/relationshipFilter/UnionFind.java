package relationshipFilter;

class UnionFind {
    private int[] parent;

    public UnionFind(int size) {
        parent = new int[size];
        // 初始化每个元素的父节点-1
        for (int i = 0; i < size; i++) {
            parent[i] = -1;
        }
    }

    // 查找元素所属的集合（根节点）
    public int collapsingFind(int i) {
        int j;
        for (j = i; parent[j] >= 0; j = parent[j]);
        while (i != j) {
            int temp = parent[i];
            parent[i] = j;
            i = temp;
        }
        return j;
    }

    // 合并两个集合
    void weightedUnion(int root1, int root2) {
        int r1 = collapsingFind(root1), r2 = collapsingFind(root2);
        int temp;
        if (r1 != r2) {
            temp = parent[r1] + parent[r2];
            if (parent[r2] < parent[r1]) {
                parent[r1] = r2;
                parent[r2] = temp;
            }
            else {
                parent[r2] = r1;
                parent[r1] = temp;
            }
        }
    }
}

