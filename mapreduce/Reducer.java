package mapreduce;

public class Reducer {

    private int userID;

    public Reducer(int userID) {
        this.userID = userID;
    }

    public int getUserID() {
        return this.userID;
    }

}
