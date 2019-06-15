package worker;

import combiner.Combiner;
import edge.Edge;
import master.Master;
import message.Message;
import utils.Communication;
import vertex.Vertex;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class Worker<L, E, V extends Message> {
    private final String id;
    protected final Map<String, Vertex<L, V>> vertices;
    protected final Map<String, List<Edge<E>>> outEdges;
    protected final Map<String, Communication<V>> vertexCommunication;
    protected Combiner combiner;
    protected Master<L, E, V> master;
    protected boolean working = true;

    public Worker(Master<L, E, V> master, String id) {
        this.master = master;
        this.id = id;
        this.vertices = new HashMap<>();
        this.outEdges = new HashMap<>();
        this.vertexCommunication = new HashMap<>();
        this.combiner = null;
    }

    public Worker(Master<L, E, V> master, String id, Combiner combiner) {
        this.master = master;
        this.id = id;
        this.vertices = new HashMap<>();
        this.outEdges = new HashMap<>();
        this.vertexCommunication = new HashMap<>();
        this.combiner = combiner;
    }

    public boolean isWorking() {
        return working;
    }

    public void setWorking(boolean working) {
        this.working = working;
    }

    public String getId() {
        return id;
    }

    public Vertex<L, V> getVertex(String vertexID) {
        return this.vertices.get(vertexID);
    }

    public abstract void addVertexIntoWorker(String vertexID, List<Edge<E>> out);

    public abstract void run(int superStep);
}
