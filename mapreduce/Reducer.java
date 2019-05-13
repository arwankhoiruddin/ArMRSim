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

    public Reducer(int userID) {
        this.taskID = userID;
    }

    public int getTaskID() {
        return this.taskID;
    }

}
