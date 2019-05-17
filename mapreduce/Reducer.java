package mapreduce;

public class Reducer extends MRTask {

    private String taskType = "Reduce";

    public String getTaskType() {
        return taskType;
    }

    public int getLength() {
        return length;
    }

    public void setLength(int length) {
        this.length = length;
    }

    public Reducer(int taskID) {
        this.taskID = taskID;
    }

    public int getTaskID() {
        return this.taskID;
    }

}
