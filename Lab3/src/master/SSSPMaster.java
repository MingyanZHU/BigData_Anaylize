package master;

import edge.Edge;
import message.IntMessage;
import worker.Worker;

import java.io.*;
import java.util.*;

public class SSSPMaster extends Master<Integer, Integer, IntMessage> {
    private static final String startNodeIndex = "0";
    private static final String SSSP_FILE_PATH = "./result/SSSP_result.txt";
    private Worker<Integer, Integer, IntMessage> startNodeWorker = null;

    @Override
    public void loadFromFile() throws IOException {
        int fileIndex = 0;
        Random random = new Random(RANDOM_SEED);
        while (true) {
            File file = new File(partitionFilePath + fileIndex + ".txt");
            if (!file.exists())
                break;
            else
                fileIndex++;

            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(new FileInputStream(file)));
            String line;
            while ((line = bufferedReader.readLine()) != null) {
                String[] nodes = line.split("\t");
                Worker<Integer, Integer, IntMessage> randomWorker = workers.get(workersList.get(random.nextInt(workersList.size())));
                if (nodes[0].equals(startNodeIndex))
                    startNodeWorker = randomWorker;
                if (nodes.length == 1) {
                    randomWorker.addVertexIntoWorker(nodes[0], Collections.emptyList());
                } else {
                    List<Edge<Integer>> edges = new ArrayList<>();
                    for (int i = 1; i < nodes.length; i++) {
                        edges.add(new Edge<>(nodes[i], 1));
                    }
                    randomWorker.addVertexIntoWorker(nodes[0], edges);
                }
            }
        }
    }

    @Override
    public void run() {
//        System.out.println(startNodeWorker.getId());
//        System.out.println(workersList.size());
//        System.out.println(workers.size());

        File outputFile = new File(SSSP_FILE_PATH);
        if (outputFile.exists() && outputFile.isFile())
            outputFile.delete();

        if (startNodeWorker == null)
            return;
        startNodeWorker.setWorking(true);
        startNodeWorker.getVertex(startNodeIndex).voteToStart();
        startNodeWorker.getVertex(startNodeIndex).setVertexValue(0);

        int superStep = 0;
        while (!this.finished) {
            this.finished = true;
            System.out.println(superStep);

            for (String workerID : this.workersList) {
                Worker<Integer, Integer, IntMessage> worker = workers.get(workerID);
                if (worker.isWorking())
                    this.finished = false;
                else
                    continue;
                worker.run(superStep);
                System.out.println(workerID + ":" + worker.getStatistician().toString());
            }
            superStep++;
            for (String workerID : this.workersList) {
                if (this.nextStepWakeUpWorkers.get(workerID)) {
                    this.nextStepWakeUpWorkers.put(workerID, false);
                    this.workers.get(workerID).setWorking(true);
                }
            }
        }
        try {
            for (Worker<Integer, Integer, IntMessage> worker : this.workers.values()) {
                worker.outputResult(SSSP_FILE_PATH);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
