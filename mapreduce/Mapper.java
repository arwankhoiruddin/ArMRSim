package mapreduce;

public class Mapper extends MRTask {

    private String taskType = "Map";

    public String getTaskType() {
        return taskType;
    }

    public Mapper(int userID) {
        this.taskID = userID;
    }

    public int getTaskID() {
        return this.taskID;
    }
    public void setLength(int length) { this.length = length; }

    public int getLength() {
        return length;
    }
}
