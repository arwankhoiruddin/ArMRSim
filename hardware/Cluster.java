package hardware;

import config.Config;
import mapreduce.DataBlock;
import mapreduce.History;
import mapreduce.Mapper;
import mapreduce.Reducer;

import java.util.ArrayList;
import java.util.Random;

public class Cluster {
    private MRNode[] nodes;
    private MRSwitch switch1;
    private MRSwitch switch2;

    public static ArrayList<History> histories = new ArrayList<>();

    // represents the Map and Reduce queue in the master's job tracker
    public static ArrayList<Mapper> mapperQueue = new ArrayList<>();
    public static ArrayList<Reducer> reducerQueue = new ArrayList<>();

    public Cluster() {

        nodes = new MRNode[Config.numNodes];
        switch1 = new MRSwitch(0, 0);
        switch2 = new MRSwitch(1, 0);

        for (int i = 0; i < Config.numNodes; i++) {
            if (Config.homogenous) {
                if (i < Config.numNodes / 2) // half of it connected to switch1
                    nodes[i] = new MRNode(1, Config.RAM, switch1);
                else // half of it connected to switch2
                    nodes[i] = new MRNode(1, Config.RAM, switch2);
            } else { // heterogeneous
                if (i < Config.numNodes / 2) // half of it connected to switch1
                    nodes[i] = new MRNode(new Random().nextDouble(), Config.RAM, switch1);
                else
                    nodes[i] = new MRNode(new Random().nextDouble(), Config.RAM, switch2);
            }
        }
    }

    public MRNode[] getNodes() {
        return this.nodes;
    }

    public int getTotalLatency(MRNode nodeFrom, MRNode nodeTo) {
        int totalLatency = 0;

        // generate additional latency (e.g. due to data collision
        int additionalLatency = new Random().nextInt(100);

        if (nodeFrom.getMrSwitch().equals(nodeTo.getMrSwitch())) { // same switch
            totalLatency = nodeFrom.getMrSwitch().getLatency();
        } else { // same router different switch
            totalLatency = nodeFrom.getMrSwitch().getLatency() + nodeTo.getMrSwitch().getLatency();
        }

        return totalLatency + additionalLatency;
    }

    public void distributeData(int[] dataByUser) {
        int numBlock = 0;
        DataBlock block = null;

        for (int idUser = 0; idUser < dataByUser.length; idUser++) {

            // each user has certain amount of data. the data has to be split into blocks
            numBlock = dataByUser[idUser] / Config.blockSize;
            if (dataByUser[idUser] % Config.blockSize != 0) numBlock++;

            // block is characterized by user id
            block = new DataBlock(idUser);

            // replicate each block as much as Config.dataReplication
            for (int rep=0; rep < Config.dataReplication; rep++) {
                int tmp = new Random().nextInt(Config.numNodes);
                nodes[tmp].addBlock(block);
            }
        }
    }
}
