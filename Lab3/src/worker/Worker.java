package worker;

import combiner.Combiner;
import edge.Edge;
import master.Master;
import message.Message;
import utils.Communication;
import utils.Statistician;
import vertex.Vertex;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public abstract class Worker<L, E, V extends Message> {
    private final String id;
    final Map<String, Vertex<L, V>> vertices;
    final Map<String, List<Edge<E>>> outEdges;
    final Map<String, Communication<V>> vertexCommunication;
    Combiner<V> combiner;
    Statistician statistician;
    Master<L, E, V> master;
    boolean working = true;

    Worker(Master<L, E, V> master, String id) {
        this.master = master;
        this.id = id;
        this.vertices = new HashMap<>();
        this.outEdges = new HashMap<>();
        this.vertexCommunication = new HashMap<>();
        this.combiner = null;
        this.statistician = new Statistician(vertices.size(), outEdges.size());
    }

    Worker(Master<L, E, V> master, String id, Combiner<V> combiner) {
        this.master = master;
        this.id = id;
        this.vertices = new HashMap<>();
        this.outEdges = new HashMap<>();
        this.vertexCommunication = new HashMap<>();
        this.combiner = combiner;
        this.statistician = new Statistician(vertices.size(), outEdges.size());
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

    public Statistician getStatistician() {
        return statistician;
    }

    public void outputResult(String filePath) throws IOException {
        BufferedWriter writer = new BufferedWriter(new FileWriter(filePath, true));
        for (Map.Entry<String, Vertex<L, V>> entry : this.vertices.entrySet()) {
            writer.write(entry.getKey() + " : " + entry.getValue().getVertexValue() + "\n");
        }
        writer.close();
    }

    void naiveAddVertexIntoWorker(String vertexID, Vertex<L, V> vertex, List<Edge<E>> out) {
        this.vertices.put(vertexID, vertex);
        this.outEdges.put(vertexID, new ArrayList<>(out));
        Communication<V> communication = new Communication<>();
        this.vertexCommunication.put(vertexID, communication);
        this.master.addVertexCommunication(vertexID, communication);
        this.statistician.setVertexNumber(this.vertices.size());
        this.statistician.setEdgeNumber(this.statistician.getEdgeNumber() + out.size());
    }

    public abstract void addVertexIntoWorker(String vertexID, List<Edge<E>> out);

    public abstract void run(int superStep);
}
