package mapreduce;

import java.util.Random;

public class Intermediary {
    private int dataID;
    private int size;

    public Intermediary(int dataID) {

        this.dataID = dataID;
        this.size = 1 + new Random().nextInt(99);
    }

    public int getDataID() { return this.dataID; }
    public int getSize() { return this.size; }
}
