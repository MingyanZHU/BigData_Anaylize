import master.SSSPMaster;
import message.IntMessage;
import master.Master;
import worker.SSSPWorker;
import worker.Worker;

import java.io.IOException;

public class Main {

    public static void main(String[] args) throws IOException {
//        System.out.println("Hello World!");
        Master<Integer, Integer, IntMessage> master = new SSSPMaster();
//        master.partition("web-Google-using.txt", 10);
//        master.save();

        for (int i = 0; i < 10; i++) {
            master.addWorker(new SSSPWorker(master, i + ""));
        }

        master.loadFromFile();
        master.run();
    }
}
