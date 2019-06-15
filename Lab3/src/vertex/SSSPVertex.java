package vertex;

import message.IntMessage;
import message.Message;

import java.util.Queue;

public class SSSPVertex extends Vertex<Integer> {
    public static final int INF = 0x3f3f3f3f;

    public SSSPVertex(String vertexID, Integer vertexValue) {
        super(vertexID, vertexValue);
    }

    @Override
    public void compute(Queue<Message> messages) {

    }

    @Override
    public IntMessage sendTo(String vertexID, Object value) {
        // todo 此处待优化 可以增加对于value的类型检查
        return new IntMessage(vertexID, (int) value);
    }
}
