package master;

import edge.Edge;
import message.Message;
import utils.Communication;
import worker.Worker;

import java.io.*;
import java.util.*;

public abstract class Master<L, E, V extends Message> {
    public static final int RANDOM_SEED = 1;
    public static final String partitionFilePath = "./partitions/";
    protected boolean finished;
    protected final List<String> workersList;
    protected final Map<String, Worker<L, E, V>> workers;
    protected final Map<String, Communication<V>> vertexCommunication;
    protected final Map<String, Set<String>> outEdges;
    protected List<Map<String, Set<String>>> cuts;

    public Master() {
        this.finished = false;
        this.workers = new HashMap<>();
        this.workersList = new ArrayList<>();
        this.vertexCommunication = new HashMap<>();
        this.outEdges = new HashMap<>();
        this.cuts = new ArrayList<>();
    }

    public boolean addWorker(Worker<L, E, V> worker) {
        workersList.add(worker.getId());
        return workers.put(worker.getId(), worker) != null;
    }

    public boolean removeWorker(String workerID) {
        if (workers.containsKey(workerID)) {
            workers.remove(workerID);
            return true;
        }
        return false;
    }

    public void partition(String filePath, int number) throws IOException {
        if (number < 1)
            return;
        BufferedReader bufferedReader = new BufferedReader(new FileReader(filePath));
        String line;
        while ((line = bufferedReader.readLine()) != null) {
            String[] ss = line.split("\t");
            String s = ss[0];
            String e = ss[1];

            if (outEdges.containsKey(s))
                outEdges.get(s).add(e);
            else {
                outEdges.put(s, new HashSet<>());
                outEdges.get(s).add(e);
            }

            if (!outEdges.containsKey(e))
                outEdges.put(e, new HashSet<>());
        }
        Random random = new Random(RANDOM_SEED);
        for (int i = 0; i < number; i++)
            cuts.add(new HashMap<>());
        for (Map.Entry<String, Set<String>> entry : outEdges.entrySet()) {
            int index = random.nextInt(number);
            cuts.get(index).put(entry.getKey(), entry.getValue());
        }
    }

    public void save() throws IOException {
        for (int i = 0; i < cuts.size(); i++) {
            BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(partitionFilePath + i + ".txt"));
            for (Map.Entry<String, Set<String>> entry : cuts.get(i).entrySet()) {
                bufferedWriter.write(entry.getKey());
                for (String dest : entry.getValue())
                    bufferedWriter.write("\t" + dest);
                bufferedWriter.write('\n');
            }
            bufferedWriter.close();
        }
    }

    public abstract void loadFromFile() throws IOException;

    public abstract void run();
}
