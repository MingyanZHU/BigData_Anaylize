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
        this.working = false;
    }

    public SSSPWorker(Master<Integer, Integer, IntMessage> master, String id, Combiner combiner) {
        super(master, id, combiner);
        this.working = false;
    }

    @Override
    public void addVertexIntoWorker(String vertexID, List<Edge<Integer>> out) {
        Vertex<Integer, IntMessage> vertex = new SSSPVertex(vertexID, SSSPVertex.INF);
        this.vertices.put(vertexID, vertex);
        this.outEdges.put(vertexID, new ArrayList<>(out));
        Communication<IntMessage> communication = new Communication<>();
        this.vertexCommunication.put(vertexID, communication);
        this.master.addVertexCommunication(vertexID, communication);
    }

    @Override
    public void run(int superStep) {
        if (working) {
            working = false;
            for (String vertexID : this.vertices.keySet()) {
                Vertex<Integer, IntMessage> vertex = this.getVertex(vertexID);
                if (vertex.isActive()) {
                    working = true;
                } else if (!this.master.getCommunicationFromVertex(vertexID).getMessagesFromLastQueue(superStep).isEmpty()) {
                    working = true;
                    vertex.voteToStart();
                } else
                    continue;
                vertex.compute(master.getCommunicationFromVertex(vertexID).getMessagesFromLastQueue(superStep));
                vertex.setSuperStep(superStep);
//                vertex.voteToHalt();
                for (Edge<Integer> outEdge : this.outEdges.get(vertexID)) {
                    IntMessage message = vertex.sendTo(outEdge.getDestinationVertex(),
                            outEdge.getEdgeValue() + vertex.getVertexValue());
                    this.master.getCommunicationFromVertex(outEdge.getDestinationVertex()).addMessageIntoQueue(message, superStep + 1);
                    this.master.wakeUpNextStep(outEdge.getDestinationVertex());
                }
            }
        }
    }
}
