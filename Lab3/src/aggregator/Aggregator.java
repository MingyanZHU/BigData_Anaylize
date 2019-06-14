package aggregator;

public abstract class Aggregator {
    public abstract void report();

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
