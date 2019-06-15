package worker;

import combiner.Combiner;
import edge.Edge;
import master.Master;
import message.IntMessage;
import utils.Communication;
import vertex.SSSPVertex;
import vertex.Vertex;

import java.util.ArrayList;
import java.util.List;

public class SSSPWorker extends Worker<Integer, Integer, IntMessage> {
    public SSSPWorker(Master<Integer, Integer, IntMessage> master, String id) {
        super(master, id);
    }

    public SSSPWorker(Master<Integer, Integer, IntMessage> master, String id, Combiner combiner) {
        super(master, id, combiner);
    }

    @Override
    public void addVertexIntoWorker(String vertexID, List<Edge<Integer>> out) {
        Vertex<Integer> vertex = new SSSPVertex(vertexID, SSSPVertex.INF);
        this.vertices.put(vertexID, vertex);
        this.outEdges.put(vertexID, new ArrayList<>(out));
        this.vertexCommunication.put(vertexID, new Communication<>());
    }
}
