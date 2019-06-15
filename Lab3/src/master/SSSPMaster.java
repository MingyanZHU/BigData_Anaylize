package master;

import edge.Edge;
import message.IntMessage;

import java.io.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

public class SSSPMaster extends Master<Integer, Integer, IntMessage> {
    @Override
    public void loadFromFile() throws IOException {
        int fileIndex = 0;
        Random random = new Random(RANDOM_SEED);
        while (true) {
            File file = new File(partitionFilePath + fileIndex + ".txt");
            if (!file.exists())
                break;

            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(new FileInputStream(file)));
            String line;
            while ((line = bufferedReader.readLine()) != null) {
                String[] nodes = line.split("\t");
                if (nodes.length == 1) {
                    workers.get(workersList.get(random.nextInt(workersList.size())))
                            .addVertexIntoWorker(nodes[0], Collections.emptyList());
                } else {
                    List<Edge<Integer>> edges = new ArrayList<>();
                    for (int i = 1; i < nodes.length; i++) {
                        edges.add(new Edge<>(nodes[i], 1));
                    }
                    workers.get(workersList.get(random.nextInt(workersList.size())))
                            .addVertexIntoWorker(nodes[0], edges);
                }
            }
        }
    }

    @Override
    public void run() {

    }
}
