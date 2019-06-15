package vertex;

import message.IntMessage;

import java.util.Queue;

public class SSSPVertex extends Vertex<Integer, IntMessage> {
    public static final int INF = 0x3f3f3f3f;

    public SSSPVertex(String vertexID, Integer vertexValue) {
        super(vertexID, vertexValue);
        voteToHalt();   // 对于单源最短路问题 初始时所有的vertex都是inactive状态
    }

    @Override
    public void compute(Queue<IntMessage> messages) {
        boolean update = false;
        while (!messages.isEmpty()) {
            IntMessage message = messages.poll();
            int minDis = message.getValue();
            if (minDis < getVertexValue()) {
                setVertexValue(minDis);
                update = true;
//                System.out.println(getVertexID() + "\t" + getVertexValue());
            }
        }
        if (update)
            voteToStart();
        else
            voteToHalt();
    }

    @Override
    public IntMessage sendTo(String vertexID, Object value) {
        if (!(value instanceof Integer))
            return null;
        return new IntMessage(vertexID, (int) value);
    }
}
