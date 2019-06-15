import message.IntMessage;
import master.Master;

import java.io.IOException;

public class Main {

    public static void main(String[] args) throws IOException {
//        System.out.println("Hello World!");
        Master<Integer, Integer, IntMessage> master = new Master<>();
        master.partition("web-Google-using.txt", 10);
        master.save();
    }
}
