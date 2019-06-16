package aggregator;

import message.DoubleMessage;
import vertex.Vertex;

public class PageRankMaxAggregator extends Aggregator<Double, DoubleMessage> {
    private double maxPageRank = Double.MIN_VALUE;
    private String maxVertexID = "";

    @Override
    public void report(Vertex<Double, DoubleMessage> vertex) {
        maxPageRank = maxPageRank > vertex.getVertexValue() ? maxPageRank : vertex.getVertexValue();
        maxVertexID = vertex.getVertexID();
    }

    @Override
    public String aggregateMessage() {
        return maxVertexID + " : " + maxPageRank;
    }
}
