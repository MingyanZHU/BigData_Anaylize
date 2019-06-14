package message;

public class IntMessage extends Message {
    private final int value;

    public IntMessage(String sendToVertexID, int value) {
        super(sendToVertexID);
        this.value = value;
    }

    @Override
    public Object getValue() {
        return this.value;
    }
}
