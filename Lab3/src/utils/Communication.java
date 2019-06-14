package utils;

import java.util.LinkedList;
import java.util.Queue;

public class Communication<V> {
    private final Queue<V> receiveQueue_1, receiveQueue_2, sendQueue;
    private boolean lastQueueIsReceiveQueue_1;

    public Communication() {
        lastQueueIsReceiveQueue_1 = true;
        receiveQueue_1 = new LinkedList<>();
        receiveQueue_2 = new LinkedList<>();
        sendQueue = new LinkedList<>();
    }

    public V getMessageValueFromLastQueue() {
        V ans;
        if (lastQueueIsReceiveQueue_1 && !receiveQueue_1.isEmpty()) {
            ans = receiveQueue_1.poll();
            if (receiveQueue_1.isEmpty())
                lastQueueIsReceiveQueue_1 = false;
        } else if (!lastQueueIsReceiveQueue_1 && !receiveQueue_2.isEmpty()) {
            ans = receiveQueue_2.poll();
            if (receiveQueue_2.isEmpty())
                lastQueueIsReceiveQueue_1 = true;
        } else {
            return null;
        }
        return ans;
    }

    public V getMessageFromSendQueue() {
        if (sendQueue.isEmpty())
            return null;
        else
            return sendQueue.poll();
    }

    public boolean addMessagseIntoSendQueue(V message) {
        return sendQueue.add(message);
    }

    public void clearLastReceiveQueue() {
        if (lastQueueIsReceiveQueue_1)
            receiveQueue_1.clear();
        else
            receiveQueue_2.clear();
    }

    public void clearSendQueue() {
        sendQueue.clear();
    }
}
