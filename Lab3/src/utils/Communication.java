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

    public Queue<V> getMessagesFromLastQueue(int superStep) {
        if (superStep % 2 == 0)
            return receiveQueue_1;
        else
            return receiveQueue_2;
    }

    public Queue<V> getMessagesFromSendQueue() {
        return this.sendQueue;
    }

    public boolean addMessageIntoSendQueue(V message) {
        return sendQueue.add(message);
    }

    public boolean addMessageIntoQueue(V message, int superStep) {
        if (superStep % 2 == 0)
            return receiveQueue_1.add(message);
        else
            return receiveQueue_2.add(message);
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
