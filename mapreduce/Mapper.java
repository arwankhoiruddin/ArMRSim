package mapreduce;

public class Mapper {

    private int userID;

    public Mapper(int userID) {
        this.userID = userID;
    }

    public int getUserID() {
        return this.userID;
    }

}
