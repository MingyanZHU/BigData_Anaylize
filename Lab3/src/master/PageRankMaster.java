package master;

import aggregator.Aggregator;
import aggregator.PageRankMaxAggregator;
import edge.Edge;
import message.DoubleMessage;
import vertex.Vertex;
import worker.Worker;

import java.io.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

public class PageRankMaster extends Master<Double, Double, DoubleMessage> {
    private Aggregator<Double, DoubleMessage> pageRankAggregator = new PageRankMaxAggregator();

    public void pageRankReport(Vertex<Double, DoubleMessage> vertex) {
        pageRankAggregator.report(vertex);
    }

    public String pageRankAggregateMessage() {
        return pageRankAggregator.aggregateMessage();
    }

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
                Worker<Double, Double, DoubleMessage> randomWorker = workers.get(workersList.get(random.nextInt(workersList.size())));
                if (nodes.length == 1) {
                    randomWorker.addVertexIntoWorker(nodes[0], Collections.emptyList());
                } else {
                    List<Edge<Double>> edges = new ArrayList<>();
                    for (int i = 1; i < nodes.length; i++) {
                        edges.add(new Edge<>(nodes[i], 1.0));
                    }
                    randomWorker.addVertexIntoWorker(nodes[0], edges);
                }
            }
        }
    }

    @Override
    public void run() {
        int superStep = 0;
        while (!this.finished) {
            this.finished = true;
            System.out.println(superStep);

            for (String workerID : this.workersList) {
                Worker<Double, Double, DoubleMessage> worker = workers.get(workerID);
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
        System.out.println(pageRankAggregator.aggregateMessage());
    }
}
