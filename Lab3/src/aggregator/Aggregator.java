package aggregator;

import message.Message;
import vertex.Vertex;

public abstract class Aggregator<L, V extends Message> {
    public abstract void report(Vertex<L, V> vertex);

    public abstract String aggregateMessage();

    public void aggregate(AggregateFunction aggregateFunction) {
        switch (aggregateFunction) {
            case MAX:
                System.out.println("max");
                break;
            case MIN:
                System.out.println("min");
                break;
            case SUM:
                System.out.println("sum");
                break;
            default:
                System.out.println("User defined");
                break;
        }
    }
}
