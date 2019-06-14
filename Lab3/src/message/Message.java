package message;

public abstract class Message {
    private final String sendToVertexID;

    Message(String sendToVertexID) {
        this.sendToVertexID = sendToVertexID;
    }

    public String getSendToVertexID() {
        return sendToVertexID;
    }

    public abstract Object getValue();
}
