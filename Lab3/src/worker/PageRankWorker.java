package worker;

import combiner.Combiner;
import edge.Edge;
import master.Master;
import master.PageRankMaster;
import message.DoubleMessage;
import vertex.PageRankVertex;
import vertex.Vertex;

import java.util.List;
import java.util.Map;

public class PageRankWorker extends Worker<Double, Double, DoubleMessage> {
    public PageRankWorker(Master<Double, Double, DoubleMessage> master, String id) {
        super(master, id);
    }

    public PageRankWorker(Master<Double, Double, DoubleMessage> master, String id, Combiner<DoubleMessage> combiner) {
        super(master, id, combiner);
    }

    @Override
    public void addVertexIntoWorker(String vertexID, List<Edge<Double>> out) {
        Vertex<Double, DoubleMessage> vertex = new PageRankVertex(vertexID, PageRankVertex.initVertexValue);
        naiveAddVertexIntoWorker(vertexID, vertex, out);
    }

    @Override
    public void run(int superStep) {
        long timeStart, timeEnd;
        int communicationNumber = 0;
        timeStart = System.currentTimeMillis();
        if (superStep >= PageRankVertex.maxStep) {
            if (this.master instanceof PageRankMaster) {
                PageRankMaster master = (PageRankMaster) this.master;
                for (Map.Entry<String, Vertex<Double, DoubleMessage>> entry : this.vertices.entrySet()) {
                    master.pageRankReport(entry.getValue());
                }
            }
            working = false;
        }
        if (working) {
            if (superStep < 1) {
                for (Map.Entry<String, Vertex<Double, DoubleMessage>> entry : this.vertices.entrySet()) {
                    this.master.report(entry.getValue());
                }
            } else {
                for (String vertexID : this.vertices.keySet()) {
                    Vertex<Double, DoubleMessage> vertex = this.getVertex(vertexID);
                    if (!this.vertexCommunication.get(vertexID).getMessagesFromLastQueue(superStep).isEmpty()) {
                        vertex.setSuperStep(superStep);
                        vertex.compute(this.vertexCommunication.get(vertexID).getMessagesFromLastQueue(superStep));
                        // todo 此处应该修改为0.15 / 全局的顶点数目 待完善Aggregator
//                        vertex.setVertexValue(0.15 / this.vertices.size() + 0.85 * vertex.getVertexValue());
                        vertex.setVertexValue(0.15 / Integer.valueOf(this.master.aggregateMessage()) + 0.85 * vertex.getVertexValue());
                    }
                    List<Edge<Double>> outEdge = this.outEdges.get(vertexID);
                    int outEdgesNumber = outEdge.size();
                    for (Edge<Double> edge : outEdge) {
                        DoubleMessage message = vertex.sendTo(edge.getDestinationVertex(), vertex.getVertexValue() / outEdgesNumber);
                        if (this.combiner == null) {
                            communicationNumber++;
                            this.master.getCommunicationFromVertex(edge.getDestinationVertex()).addMessageIntoQueue(message, superStep);
                        } else {
                            this.combiner.combine(edge.getDestinationVertex(), message);
                        }
                    }
                }
            }
        }
        if (this.combiner != null) {
            for (Map.Entry<String, DoubleMessage> entry : this.combiner.getCombineMessages().entrySet()) {
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
