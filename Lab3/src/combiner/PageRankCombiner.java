package combiner;

import message.DoubleMessage;

public class PageRankCombiner extends Combiner<DoubleMessage> {
    @Override
    public void combine(String destVertexID, DoubleMessage message) {
        if (!this.combineMessages.containsKey(destVertexID)) {
            this.combineMessages.put(destVertexID, message);
        } else {
            this.combineMessages.put(destVertexID, new DoubleMessage(destVertexID,
                    message.getValue() + this.getCombineMessages().get(destVertexID).getValue()));
        }
    }
}
