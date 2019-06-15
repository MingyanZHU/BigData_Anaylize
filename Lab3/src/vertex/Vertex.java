package vertex;

import message.Message;

import java.util.Queue;

public abstract class Vertex<L, V extends Message> {
    // L顶点属性的泛型
    private String vertexID;
    private L vertexValue;
    private boolean active;
    private int superStep;

    public Vertex(String vertexID, L vertexValue) {
        this.vertexID = vertexID;
        this.vertexValue = vertexValue;
        this.active = true;
        this.superStep = 0;
    }

    public abstract void compute(Queue<V> messages);

    public String getVertexID() {
        return vertexID;
    }

    public L getVertexValue() {
        return vertexValue;
    }

    public void setVertexValue(L vertexValue) {
        this.vertexValue = vertexValue;
    }

    public void voteToHalt() {
        this.active = false;
    }

    public void voteToStart() {
        this.active = true;
    }

    public boolean isActive() {
        return active;
    }

    public int getSuperStep() {
        return superStep;
    }


    public void setSuperStep(int superStep) {
        this.superStep = superStep;
    }

//    public void superStepPlus() {
//        this.superStep++;
//    }

    public abstract V sendTo(String vertexID, Object value);

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Vertex)) return false;

        Vertex<?, ?> vertex = (Vertex<?, ?>) o;

        return getVertexID().equals(vertex.getVertexID());

    }

    @Override
    public int hashCode() {
        return getVertexID().hashCode();
    }
}
