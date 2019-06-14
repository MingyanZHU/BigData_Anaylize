package utils;

import message.IntMessage;
import message.Message;

import java.util.LinkedList;
import java.util.Queue;

public class Communication<V extends Message> {
    private final Queue<V> receiveQueue_1, receiveQueue_2, sendQueue;

    public Communication() {
        receiveQueue_1 = new LinkedList<>();
        receiveQueue_2 = new LinkedList<>();
        sendQueue = new LinkedList<>();
    }

    public V getMessageValueFromLastQueue(int superStep) {
        V ans;
        if (superStep % 2 == 0 && !receiveQueue_1.isEmpty()) {
            ans = receiveQueue_1.poll();
        } else if (superStep % 2 == 1 && !receiveQueue_2.isEmpty()) {
            ans = receiveQueue_2.poll();
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

    public void clearLastReceiveQueue(int superStep) {
        if (superStep % 2 == 0)
            receiveQueue_1.clear();
        else
            receiveQueue_2.clear();
    }

    public void clearSendQueue() {
        sendQueue.clear();
    }
}
