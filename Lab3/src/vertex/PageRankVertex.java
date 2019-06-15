package vertex;

import message.DoubleMessage;

import java.util.Queue;

public class PageRankVertex extends Vertex<Double, DoubleMessage> {
    public static final int maxStep = 30;
    public static final double initVertexValue = 1.0;

    public PageRankVertex(String vertexID, Double vertexValue) {
        super(vertexID, vertexValue);
    }

    @Override
    public void compute(Queue<DoubleMessage> messages) {
        if (this.getSuperStep() < maxStep) {
            double sum = 0;
            while (!messages.isEmpty()) {
                double value = messages.poll().getValue();
                sum += value;
            }
            this.setVertexValue(sum);
        } else {
            this.voteToHalt();
        }
    }

    @Override
    public DoubleMessage sendTo(String vertexID, Object value) {
        if (!(value instanceof Double))
            return null;
        return new DoubleMessage(vertexID, (double) value);
    }
}
