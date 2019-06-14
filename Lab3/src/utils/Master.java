package utils;

import message.Message;
import vertex.Vertex;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Master<L, E, V extends Message> {
    private boolean finished;
    private final List<Worker<L, E, V>> workers;
    private final Map<String, Communication<V>> vertexCommunication;

    public Master() {
        this.finished = false;
        this.workers = new ArrayList<>();
        this.vertexCommunication = new HashMap<>();
    }

    public boolean addWorker(Worker<L, E, V> worker) {
        return workers.add(worker);
    }

    public boolean removeWorker(String workerID) {
        Worker<L, E, V> workerToFind = null;
        for (Worker<L, E, V> worker : workers) {
            if (worker.getId().equals(workerID))
                workerToFind = worker;
        }
        if (workerToFind == null)
            return false;
        return workers.remove(workerToFind);
    }

    public void partition() {
        // todo
    }

    public void save(String filePath) {
        // todo
    }

    public void load(String filePath) {
        // todo
    }

    public void run() {
        // TODO Master.run()
    }
}
