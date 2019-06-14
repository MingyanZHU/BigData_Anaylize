package message;

public class DoubleMessage extends Message {
    private final double value;

    public DoubleMessage(String sendToVertexID, double value) {
        super(sendToVertexID);
        this.value = value;
    }

    @Override
    public Object getValue() {
        return this.value;
    }
}
