package aggregator;

import message.DoubleMessage;
import vertex.Vertex;

import java.util.PriorityQueue;

public class PageRankMaxAggregator extends Aggregator<Double, DoubleMessage> {
    private static final int N = 10;
//    private double maxPageRank = Double.MIN_VALUE;
//    private String maxVertexID = "";

    private PriorityQueue<Vertex<Double, DoubleMessage>> top;

    public PageRankMaxAggregator() {
        this.top = new PriorityQueue<>(N, (o1, o2) -> (int) (o1.getVertexValue() - o2.getVertexValue()));
    }

    @Override
    public void report(Vertex<Double, DoubleMessage> vertex) {
//        if (maxPageRank < vertex.getVertexValue()) {
//            maxPageRank = vertex.getVertexValue();
//            maxVertexID = vertex.getVertexID();
//        }
        if (top.size() < N) {
            top.add(vertex);
        } else {
            if (top.peek().getVertexValue() < vertex.getVertexValue()) {
                top.poll();
                top.add(vertex);
            }
        }
    }

    @Override
    public String aggregateMessage() {
        StringBuilder stringBuilder = new StringBuilder();
        Vertex[] vertices = new Vertex[N];
        top.toArray(vertices);
        for (Vertex vertex : vertices) {
            stringBuilder.append(vertex.getVertexID()).append(" : ").append(vertex.getVertexValue()).append("\n");
        }
        return stringBuilder.toString();
    }
}
