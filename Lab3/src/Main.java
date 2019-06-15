import combiner.PageRankCombiner;
import combiner.SSSPCombiner;
import master.PageRankMaster;
import master.SSSPMaster;
import message.DoubleMessage;
import message.IntMessage;
import master.Master;
import worker.PageRankWorker;
import worker.SSSPWorker;
import worker.Worker;

import java.io.IOException;

public class Main {

    public static void main(String[] args) throws IOException {
        long timeStart, timeEnd;
        timeStart = System.currentTimeMillis();
//        Master<Integer, Integer, IntMessage> master = new SSSPMaster();
        Master<Double, Double, DoubleMessage> master = new PageRankMaster();
//        master.partition("web-Google-using.txt", 10);
//        master.save();

        for (int i = 0; i < 10; i++) {
//            master.addWorker(new SSSPWorker(master, i + "", new SSSPCombiner()));
//            master.addWorker(new SSSPWorker(master, i + ""));
//            master.addWorker(new PageRankWorker(master, i + ""));
            master.addWorker(new PageRankWorker(master, i + "", new PageRankCombiner()));
        }

        master.loadFromFile();
        master.run();
        timeEnd = System.currentTimeMillis();
        System.err.println((timeEnd - timeStart) / 1000.0);
    }
}
