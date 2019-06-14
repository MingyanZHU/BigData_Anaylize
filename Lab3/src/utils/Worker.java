package utils;

import edge.Edge;
import vertex.Vertex;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Worker<L, E, V> {
    private final Map<Vertex<L>, List<Edge<L, E>>> outEdges;
    private final Map<Vertex<L>, Communication<V>> vertexCommunication;
    private boolean active;

    public Worker() {
        this.outEdges = new HashMap<>();
        this.vertexCommunication = new HashMap<>();
        this.active = true;
    }

    public boolean isActive() {
        return active;
    }

    public void sleep() {
        this.active = false;
    }

    public void active() {
        this.active = true;
    }
}
