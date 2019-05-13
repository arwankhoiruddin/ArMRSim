package mapreduce;

public class DataBlock {
    private int blockNumber;

    public DataBlock(int userNumber) {
        this.blockNumber = userNumber;
    }

    public int getBlockNumber() {
        return this.blockNumber;
    }
}
