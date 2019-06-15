package combiner;

import message.Message;

import java.util.HashMap;
import java.util.Map;

public abstract class Combiner<V extends Message> {
    final Map<String, V> combineMessages = new HashMap<>();

    public Map<String, V> getCombineMessages() {
        return combineMessages;
    }

    public void clear() {
        this.combineMessages.clear();
    }

    public abstract void combine(String destVertexID, V message);
}
