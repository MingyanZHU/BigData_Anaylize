package combiner;

import message.IntMessage;

public class SSSPCombiner extends Combiner<IntMessage> {
    @Override
    public void combine(String destVertexID, IntMessage message) {
        if (!this.combineMessages.containsKey(destVertexID) || this.combineMessages.get(destVertexID).getValue() > message.getValue()) {
            this.combineMessages.put(destVertexID, message);
        }
    }
}
