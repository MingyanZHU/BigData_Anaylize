package worker;

import combiner.Combiner;
import edge.Edge;
import master.Master;
import message.IntMessage;
import vertex.SSSPVertex;
import vertex.Vertex;

import java.io.*;
import java.util.List;
import java.util.Map;

public class SSSPWorker extends Worker<Integer, Integer, IntMessage> {
    public SSSPWorker(Master<Integer, Integer, IntMessage> master, String id) {
        super(master, id);
        this.working = false;
    }

    public SSSPWorker(Master<Integer, Integer, IntMessage> master, String id, Combiner<IntMessage> combiner) {
        super(master, id, combiner);
        this.working = false;
    }

    @Override
    public void addVertexIntoWorker(String vertexID, List<Edge<Integer>> out) {
        Vertex<Integer, IntMessage> vertex = new SSSPVertex(vertexID, SSSPVertex.INF);
        naiveAddVertexIntoWorker(vertexID, vertex, out);
    }

    @Override
    public void outputResult(String filePath) throws IOException {
        BufferedWriter writer = new BufferedWriter(new FileWriter(filePath, true));
        for (Map.Entry<String, Vertex<Integer, IntMessage>> entry : this.vertices.entrySet()) {
            writer.write(entry.getKey() + " : " + (entry.getValue().getVertexValue() >= SSSPVertex.INF ? "INF" : entry.getValue().getVertexValue()) + "\n");
        }
        writer.close();
    }

    @Override
    public void run(int superStep) {
        long timeStart, timeEnd;
        int communicationNumber = 0;
        timeStart = System.currentTimeMillis();
        if (working) {
            working = false;
            for (String vertexID : this.vertices.keySet()) {
                Vertex<Integer, IntMessage> vertex = this.getVertex(vertexID);
                if (!this.master.getCommunicationFromVertex(vertexID).getMessagesFromLastQueue(superStep).isEmpty()) {
                    vertex.setSuperStep(superStep);
                    vertex.compute(master.getCommunicationFromVertex(vertexID).getMessagesFromLastQueue(superStep));
                }
                if (vertex.isActive())
                    working = true;
                else
                    continue;
                for (Edge<Integer> outEdge : this.outEdges.get(vertexID)) {
                    IntMessage message = vertex.sendTo(outEdge.getDestinationVertex(),
                            outEdge.getEdgeValue() + vertex.getVertexValue());
                    this.master.wakeUpNextStep(outEdge.getDestinationVertex());
                    if (this.combiner == null) {
                        communicationNumber++;
                        this.master.getCommunicationFromVertex(outEdge.getDestinationVertex()).addMessageIntoQueue(message, superStep + 1);
                    } else {
                        this.combiner.combine(outEdge.getDestinationVertex(), message);
                    }
                }
                vertex.voteToHalt();
            }
        }
        if (this.combiner != null) {
            for (Map.Entry<String, IntMessage> entry : this.combiner.getCombineMessages().entrySet()) {
                communicationNumber++;
                this.master.getCommunicationFromVertex(entry.getKey()).addMessageIntoQueue(entry.getValue(), superStep + 1);
            }
            this.combiner.clear();
        }
        timeEnd = System.currentTimeMillis();
        this.statistician.setCommunicationNumber(communicationNumber);
        this.statistician.setSuperStep(superStep);
        this.statistician.setTimeUsed((timeEnd - timeStart) / 1000.0);
    }
}
