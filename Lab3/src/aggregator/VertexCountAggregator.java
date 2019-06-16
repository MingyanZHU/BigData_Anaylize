package aggregator;

import vertex.Vertex;

public class VertexCountAggregator extends Aggregator {
    private int count = 0;

    @Override
    public void report(Vertex vertex) {
        aggregate(AggregateFunction.SUM);
    }

    @Override
    public String aggregateMessage() {
        return String.valueOf(count);
    }

    @Override
    public void aggregate(AggregateFunction aggregateFunction) {
        if (aggregateFunction == AggregateFunction.SUM) {
            count++;
        }
    }
}
