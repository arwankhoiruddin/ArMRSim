package mapreduce;

import java.time.Instant;

public class History {
    private MRTask task;
    int length;

    public History(MRTask task, int length) {
        this.task = task;
        this.length = length;
    }

    public MRTask getTask() {
        return task;
    }

    public int getLength() {
        return length;
    }
}
