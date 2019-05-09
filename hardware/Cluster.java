package hardware;

import config.Config;
import mapreduce.DataBlock;

import java.util.Random;

public class Cluster {
    private MRNode[] nodes;
    private MRSwitch switch1;
    private MRSwitch switch2;

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

        if (nodeFrom.getMrSwitch().equals(nodeTo.getMrSwitch())) {
            totalLatency = nodeFrom.getMrSwitch().getLatency();
        } else {
            totalLatency = nodeFrom.getMrSwitch().getLatency() + nodeTo.getMrSwitch().getLatency();
        }

        return totalLatency;
    }

    public void distributeData(int[] dataByUser) {
        int numBlock = 0;
        DataBlock block = null;

        int allBlocks = 0;

        for (int idUser = 0; idUser < dataByUser.length; idUser++) {

            // each user has certain amount of data. the data has to be split into blocks
            numBlock = dataByUser[idUser] / Config.blockSize;
            if (dataByUser[idUser] % Config.blockSize != 0) numBlock++;

            // block is characterized by user id
            block = new DataBlock(idUser);

            numBlock *= Config.dataReplication;

            allBlocks += numBlock;

            for (int idBlock = 0; idBlock < numBlock; idBlock++) {

                // maybe randomize the block placement is better
                nodes[new Random().nextInt(Config.numNodes)].addBlock(block);
            }
        }
    }
}
