package vertex;

public abstract class Vertex<L> {
    // L顶点属性的泛型
    private String vertexID;
    private L vertexValue;

    public Vertex(String vertexID, L vertexValue) {
        this.vertexID = vertexID;
        this.vertexValue = vertexValue;
    }

    public abstract void compute();

    public String getVertexID() {
        return vertexID;
    }

    public L getVertexValue() {
        return vertexValue;
    }

    public void setVertexValue(L vertexValue) {
        this.vertexValue = vertexValue;
    }

    public void setActive() {
    }


    // 判断两个Vertex实例是否相等与其当前的状态无关
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Vertex)) return false;

        Vertex<?> vertex = (Vertex<?>) o;

        if (!getVertexID().equals(vertex.getVertexID())) return false;
        return getVertexValue().equals(vertex.getVertexValue());

    }

    @Override
    public int hashCode() {
        int result = getVertexID().hashCode();
        result = 31 * result + getVertexValue().hashCode();
        return result;
    }
}
