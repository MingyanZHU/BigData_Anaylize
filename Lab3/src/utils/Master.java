package utils;

import java.util.ArrayList;
import java.util.List;

public class Master<L, E, V> {
    private final List<Worker<L, E, V>> workers;

    public Master() {
        workers = new ArrayList<>();
    }

    public void run(){
        // TODO Master.run()
    }
}
