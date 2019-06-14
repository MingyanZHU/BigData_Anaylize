package utils;

import combiner.Combiner;
import edge.Edge;
import message.Message;
import vertex.Vertex;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Worker<L, E, V extends Message> {
    private final String id;
    private final Map<String, Vertex<L>> vertices;
    private final Map<String, List<Edge<E>>> outEdges;
    private final Map<String, Communication<V>> vertexCommunication;
    private Combiner combiner;
    private Master<L, E, V> master;

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

    public String getId() {
        return id;
    }
}
