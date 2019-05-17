package mapreduce;

import config.Config;
import hardware.Cluster;
import hardware.MRNode;
import org.apache.commons.math3.distribution.ZipfDistribution;

import java.lang.reflect.Array;
import java.util.*;

import static config.Config.cluster;
import static config.Config.numNodes;

public class Scheduler {

    public static int duration;

    public static void scheduleMap(Cluster cluster, Mapper[] mappers) {

        switch (Config.mapScheduler) {
            case FIFO:
                FIFOMapScheduler(cluster, mappers);
                break;
        }
    }

    private static void FIFOMapScheduler(Cluster cluster, Mapper[] mappers) {
        // add mapper into the nodes based on the arrival
        // and just add into the nodes that have empty map slot
        MRNode[] nodes = cluster.getNodes();
        for (int i=0; i<mappers.length; i++) {

            int nodeNumber = 0;
            while (true) {
                if (nodeNumber == cluster.getNodes().length)
                    nodeNumber = 0;
                if (nodes[nodeNumber].hasMapSlot()) {
                    nodes[nodeNumber].addMap(mappers[i]);
                    break;
                }
                nodeNumber++;
            }
        }
    }

    private static void FIFOMapScheduler() {
        // schedule to all available slots in all nodes

        MRNode[] nodes = cluster.getNodes();

        int nodeNum = 0;
        while (nodeNum < Config.numNodes) {

            System.out.println("Node number: " + nodeNum);

            if (nodes[nodeNum].hasMapSlot()) {

                // only allocate if there is map task in queue
                if (Cluster.mapperQueue.size() > 0) {

                    Mapper allocatedMap = Cluster.mapperQueue.get(0);
                    nodes[nodeNum].addMap(allocatedMap);

                    System.out.println("Map number: " + allocatedMap.getTaskID());

                    Cluster.mapperQueue.remove(allocatedMap);
                }
            }

            nodeNum++;
        }
    }

    private static int speculateMapper(Cluster cluster, int currentNode, Mapper mapper) {
        MRNode[] nodes = cluster.getNodes();

        int nodeNumber = 0;

        while (true) {
            if (nodeNumber == cluster.getNodes().length)
                nodeNumber = 0;

            if (nodeNumber != currentNode) {
                if (nodes[nodeNumber].hasMapSlot()) {
                    System.out.println("Mapper " + mapper.getTaskID() + " is speculated to node " + nodeNumber);
                    nodes[nodeNumber].addMap(mapper);

                    // add the length of the task with the copying time
                    mapper.setLength(mapper.getLength() + cluster.getTotalLatency(nodes[currentNode], nodes[nodeNumber]));
                    return nodeNumber;
                }
            }

            nodeNumber++;
        }
    }

    public static void runScheduleMapper(Cluster cluster) {

        while (!Cluster.mapperQueue.isEmpty()) {

            // schedule mappers in queue as long as there is/are slot(s) in nodes. if there is slot to run, then delete it from mapperQueue
            switch (Config.mapScheduler) {
                case FIFO:
                    FIFOMapScheduler();
                    break;
            }

            // now run the scheduled map
            runMapper();
        }
    }

    public static void runMapper() {
        System.out.println("Start running mapper");
        MRNode[] mrNodes = cluster.getNodes();
        int maxLengthRun = 1000;

        // speculation threshold
        double specThres = 0.7;

        // run all mappers in each nodes

        int remainingMapper = -1;

        while (remainingMapper != 0) {

            remainingMapper = 0;

            for (int nodeNum=0; nodeNum < mrNodes.length; nodeNum++) {

                System.out.println("=============");
                System.out.println("Node number: " + nodeNum);
                System.out.println("Number of mappers in this node: " + mrNodes[nodeNum].getMapSlot().size());

                for (int i=0; i<mrNodes[nodeNum].getDataBlocks().size(); i++) {
                    System.out.println("Block in this node: " + mrNodes[nodeNum].getDataBlocks().get(i).getBlockNumber());
                }

                for (int i=0; i<mrNodes[nodeNum].getMapSlot().size(); i++) {
                    System.out.println("Mappers in this node: " + mrNodes[nodeNum].getMapSlot().get(i).getTaskID());
                }

                // run if there is mappers in the node
                while (mrNodes[nodeNum].getMapSlot().size() > 0) {

                    ArrayList<Mapper> mappers = mrNodes[nodeNum].getMapSlot();

                    // randomize the length of maps in the nodes
                    ZipfDistribution zipfDistribution = new ZipfDistribution(maxLengthRun, 0.5);
                    int[] mapLengths = zipfDistribution.sample(mappers.size());

                    ArrayList<Integer> mapRunLengths = new ArrayList<Integer>();
                    for (int t : mapLengths) {
                        mapRunLengths.add(t);
                    }

                    // run the map if it has the block needed
                    for (int i=0; i<mrNodes[nodeNum].getMapSlot().size(); i++) {
                        if (mrNodes[nodeNum].getMapSlot().size() > 0) {
                            Mapper runningMapper = mappers.get(0);
                            if (mrNodes[nodeNum].hasBlockNeeded(runningMapper.getTaskID())) {
                                // mapper will create intermediary results
                                int maxInterm = 20;

                                int numIntermediary = 10 + new Random().nextInt(maxInterm);

                                runningMapper.setLength(mapRunLengths.get(0));
                                int progress = new Random().nextInt(mapRunLengths.get(0)); // generate random current progress
                                double progressRate = new Random().nextDouble(); // generate random progress rate for each mapper

                                // LATE algorithm
                                double progressScore = (double) progress / mapRunLengths.get(0);

                                double specDec = ((1 - progressScore) / progressRate);

                                System.out.println("progressScore: " + progressScore);
                                System.out.println("specDec: " + specDec);

                                if (specDec < specThres) {
                                    System.out.println("Running mapper ID: " + runningMapper.getTaskID());

                                    // generate the intermediary
                                    for (int interm=0; interm < numIntermediary; interm++) {
                                        Intermediary intermediary = new Intermediary(new Random().nextInt(1 + interm));
                                        mrNodes[nodeNum].addIntermediary(intermediary);
                                    }

                                    // also don't forget to put it in history
                                    Cluster.histories.add(new History(runningMapper, runningMapper.getLength()));
                                } else {
                                    int newNode = speculateMapper(cluster, nodeNum, runningMapper);

                                    // generate the intermediary
                                    for (int interm=0; interm < numIntermediary; interm++) {
                                        Intermediary intermediary = new Intermediary(new Random().nextInt(1 + interm));
                                        mrNodes[newNode].addIntermediary(intermediary);
                                    }
                                }

                                mrNodes[nodeNum].deleteMapper(runningMapper);
                                mapRunLengths.remove(0);

                            } else { // the node does not have the block needed. Need to copy to the node containing the block

                                int node;
                                if (nodeNum == mrNodes.length-1)
                                    node = 0;
                                else
                                    node = nodeNum + 1;

                                while (true) {

                                    if (node == mrNodes.length - 1)
                                        node = 0;

                                    if (mrNodes[node].hasBlockNeeded(runningMapper.getTaskID()))
                                        if (mrNodes[node].hasMapSlot()) {
                                            System.out.println("Map ID " + runningMapper.getTaskID() + " moved from " + nodeNum + " to node " + node);
                                            mrNodes[node].addMap(runningMapper);
                                            mrNodes[nodeNum].deleteMapper(runningMapper);

                                            // total time needed
                                            int transferTime = cluster.getTotalLatency(mrNodes[node], mrNodes[nodeNum]);
                                            Cluster.histories.add(new History(runningMapper, runningMapper.getLength() + transferTime));

                                            break;
                                        }
                                    node++;
                                }
                            }
                        }

                    }
                }
            }

            for (int i=0; i<Config.numNodes; i++) {
                remainingMapper += mrNodes[i].getMapSlot().size();
            }
            System.out.println("Remaining mappers need to run: " + remainingMapper);
        }

    }

    private static void speculateReducer(Cluster cluster, int currentNode, Reducer reducer) {
        MRNode[] nodes = cluster.getNodes();

        int nodeNumber = 0;

        while (true) {
            if (nodeNumber != currentNode) {
                if (nodes[nodeNumber].hasReduceSlot()) {
                    nodes[nodeNumber].addReduce(reducer);
                    break;
                }
            }
            if (nodeNumber == cluster.getNodes().length)
                nodeNumber = 0;

            nodeNumber++;
        }
    }

    public static void runScheduleReducer(Cluster cluster) {
        // sort and copy phase - sorting the intermediaries
        MRNode[] nodes = cluster.getNodes();

        int intermediaryPartition = 0;
        ArrayList<Integer> allInterms = new ArrayList<>();

        for (int i = 0; i < Config.numNodes; i++) {
            ArrayList<Intermediary> intermediaries = nodes[i].getIntermediaries();
            for (Intermediary intermediary : intermediaries) {
                allInterms.add(intermediary.getDataID());
            }

            intermediaryPartition += intermediaries.size();
        }

        // sort the intermediaries
        Collections.sort(allInterms);

        int intermNum = allInterms.get(0);
        int numCount = 0;
        int numReducer = 1;

        // distribute the sorted intermediaries into the nodes
        // I think I can do something here for my research

        for (int i=0; i<allInterms.size(); i++) {
            if (allInterms.get(i) != intermNum) {
                intermNum = allInterms.get(i);

                // add reducers
                Reducer reducer = new Reducer(intermNum);
                reducer.setLength(numCount);
                Cluster.reducerQueue.add(reducer);

                numReducer++;
                numCount = 0;
            } else {
                numCount++;
            }
        }

        // now schedule the reducers
        scheduleReducer(cluster);
    }


//    public static void scheduleReducer() {
//
//        while (!Cluster.reducerQueue.isEmpty()) {
//            switch (Config.reduceScheduler) {
//                case FIFO:
//                    FIFOReduceScheduler(cluster);
//                    break;
//                case LBBS:
//                    LBBSAlgorithm(allInterms, intermediaryPartition, nodes);
//                    break;
//            }
//        }
//
//    }

    public static void scheduleReducer(Cluster cluster) {

        // sort and shuffle phase - sorting the intermediaries
        MRNode[] nodes = cluster.getNodes();

        int intermediaryPartition = 0;
        ArrayList<Integer> allInterms = new ArrayList<>();

        for (int i = 0; i < Config.numNodes; i++) {
            ArrayList<Intermediary> intermediaries = nodes[i].getIntermediaries();
            for (Intermediary intermediary : intermediaries) {
                allInterms.add(intermediary.getDataID());
            }

            intermediaryPartition += intermediaries.size();
        }

        while (!Cluster.reducerQueue.isEmpty()) {

            switch (Config.reduceScheduler) {
                case FIFO:
                    FIFOReduceScheduler(cluster);
                    break;
                case LBBS:
                    LBBSAlgorithm(allInterms, intermediaryPartition, nodes);
                    break;
            }

        }
    }

    private static void FIFOReduceScheduler(Cluster cluster) {
        // add reducer into the nodes based on the arrival
        // and just add into the nodes that have empty map slot
        MRNode[] nodes = cluster.getNodes();
//        for (int i=0; i<mappers.length; i++) {
//
//            int nodeNumber = 0;
//            while (true) {
//                if (nodeNumber == cluster.getNodes().length)
//                    nodeNumber = 0;
//                if (nodes[nodeNumber].hasMapSlot()) {
//                    nodes[nodeNumber].addMap(mappers[i]);
//                    break;
//                }
//                nodeNumber++;
//            }
//        }
    }
//
//    public static void LBBSAlgorithm(Cluster cluster) {
//
//        // LBBS Algorithm
//        // generate the locality matrix
//
//        MRNode[] nodes = cluster.getNodes();
//
//        int maxInterms = 0;
//        for (int i=0; i < nodes.length; i++) {
//            for (Intermediary intermediary: nodes[i].getIntermediaries()) {
//                if (maxInterms < intermediary.getDataID())
//                    maxInterms = intermediary.getDataID();
//            }
//        }
//
//        System.out.println("max intermediate: " + maxInterms);
//
//        int tmp = 0;
//        int[][] localMatrix = new int[maxInterms][Config.numNodes];
//        for (int i=0; i<Config.numNodes; i++) {
//            ArrayList<Intermediary> intermediaries = nodes[i].getIntermediaries();
//            for (Intermediary intermediary: intermediaries) {
//                localMatrix[intermediary.getDataID()][i] = 1;
//            }
//        }
//
//
//        double[][] locality = new double[numReducers][Config.numNodes];
//
//        // calculate the locality
//        for (int i=0; i<Config.numNodes; i++) {
//            for (int j=0; j<allInterms.size(); j++) {
//                int tmp = 0;
//                for (int k=0; k<Config.numNodes; k++) {
//                    tmp += localMatrix[j][k];
//                }
//                double locality1 = 0;
//                if (tmp != 0)
//                    locality1 = localMatrix[j][i] / tmp;
//
//                tmp = 0;
//                for (int k=0; k<allInterms.size(); k++) {
//                    tmp += localMatrix[k][i];
//                }
//                double locality2 = 0;
//                if (tmp != 0)
//                    locality2 = localMatrix[j][i] / tmp;
//                locality[j][i] = locality1 * locality2;
//            }
//        }
//
//        // sort the intermediaries
//        Collections.sort(allInterms);
//
//        ArrayList<Reducer> reducers = new ArrayList<>();
//
//        // calculate the number of reducers generated
//        int numReduce = 0;
//        int t = allInterms.get(0);
//        for (int n:allInterms) {
//            if (n != t) {
//                t = n;
//                numReduce++;
//            }
//        }
//
//        System.out.println("Number of reducer: " + numReduce);
//
//        // find the workloads for each nodes
//        ArrayList<Integer> heap = new ArrayList<>();
//
//        for (int n=0; n<Config.numNodes; n++) {
//            int tmp = 0;
//            for (int p=0; p<intermediaryPartition; p++) {
//                tmp += localMatrix[p][n];
//            }
//            heap.add(tmp);
//        }
//
//        // distribute the reducers
//        while (heap.size() > 0) {
//            int min = 1000000;
//            for (int temp: heap) {
//                if (temp < min)
//                    min = temp;
//            }
//
//            // distribute Reducer while still any
//            if (numReduce > 0) {
//                System.out.println("allocating reducer to node number " + heap.indexOf(min));
//                nodes[heap.indexOf(min)].addReduce(new Reducer(new Random().nextInt(numReduce)));
//            }
//
//            heap.remove((Integer) min);
//        }
//    }

    public static void LBBSAlgorithm(ArrayList<Integer> allInterms, int intermediaryPartition, MRNode[] nodes) {

        // LBBS Algorithm
        // generate the locality matrix
        int[][] localMatrix = new int[allInterms.size()][Config.numNodes];
        for (int i=0; i<Config.numNodes; i++) {
            ArrayList<Intermediary> intermediaries = nodes[i].getIntermediaries();
            for (Intermediary intermediary: intermediaries) {
                localMatrix[intermediary.getDataID()][i] = 1;
            }
        }

        double[][] locality = new double[allInterms.size()][Config.numNodes];

        // calculate the locality
        for (int i=0; i<Config.numNodes; i++) {
            for (int j=0; j<allInterms.size(); j++) {
                int tmp = 0;
                for (int k=0; k<Config.numNodes; k++) {
                    tmp += localMatrix[j][k];
                }
                double locality1 = 0;
                if (tmp != 0)
                    locality1 = localMatrix[j][i] / tmp;

                tmp = 0;
                for (int k=0; k<allInterms.size(); k++) {
                    tmp += localMatrix[k][i];
                }
                double locality2 = 0;
                if (tmp != 0)
                    locality2 = localMatrix[j][i] / tmp;
                locality[j][i] = locality1 * locality2;
            }
        }

        // sort the intermediaries
        Collections.sort(allInterms);

        ArrayList<Reducer> reducers = new ArrayList<>();

        // calculate the number of reducers generated
        int numReduce = 0;
        int t = allInterms.get(0);
        for (int n:allInterms) {
            if (n != t) {
                t = n;
                numReduce++;
            }
        }

        // find the workloads for each nodes
        ArrayList<Integer> heap = new ArrayList<>();

        for (int n=0; n<Config.numNodes; n++) {
            int tmp = 0;
            for (int p=0; p<intermediaryPartition; p++) {
                tmp += localMatrix[p][n];
            }
            heap.add(tmp);
        }

        // distribute the reducers
        while (heap.size() > 0) {
            int min = 1000000;
            for (int temp: heap) {
                if (temp < min)
                    min = temp;
            }

            // distribute Reducer while still any
            if (numReduce > 0) {
                System.out.println("allocating reducer to node number " + heap.indexOf(min));
                if (Cluster.reducerQueue.size() > 0) {
                    nodes[heap.indexOf(min)].addReduce(Cluster.reducerQueue.get(0));
                    Cluster.reducerQueue.remove(0);
                }
            }

            heap.remove((Integer) min);
        }

    }

    public static void runReducer(Cluster cluster) {

        // the length of reducer is using Zipf distribution
        MRNode[] mrNodes = cluster.getNodes();
        int maxLengthRun = 100000;

        // speculation threshold
        double specThres = 0.7;

        // keep running while there is still reducers in any nodes
        int numReducers = 100000;

        while (numReducers != 0) {

            for (int i = 0; i < mrNodes.length; i++) {

                // there are three phases in reducer i.e. copy, shuffle and reduce
                // copy take most of the time compared to shuffle and reduce
                // so here we put weight for those three and generate zipf function of the length based on the weight
                double copyPhase = 0.6;
                double shufflePhase = 0.2;
                double reducePhase = 0.2;

                ZipfDistribution copyZipf = new ZipfDistribution((int) Math.round(copyPhase * maxLengthRun), 0.5);
                ZipfDistribution shuffleZipf = new ZipfDistribution((int) Math.round(shufflePhase * maxLengthRun), 0.5);
                ZipfDistribution reduceZipf = new ZipfDistribution((int) Math.round(reducePhase * maxLengthRun), 0.5);

                if (mrNodes[i].getReduceSlot().size() > 0) {
                    int[] copyRunLengths = copyZipf.sample(mrNodes[i].getReduceSlot().size());
                    int[] shuffleRunLengths = shuffleZipf.sample(mrNodes[i].getReduceSlot().size());
                    int[] reduceRunLengths = reduceZipf.sample(mrNodes[i].getReduceSlot().size());

                    ArrayList<Reducer> reducers = mrNodes[i].getReduceSlot();

                    for (int j = 0; j < reduceRunLengths.length; j++) {
                        int size = mrNodes[i].getReduceSlot().size();
                        Reducer runningReducer = reducers.get(0); // take the first one

                        // rTuner algorithm
                        double progressScore;
                        double specDec;

                        // copy phase
                        int progress = new Random().nextInt(copyRunLengths[j]); // generate random current progress
                        int progressRate = new Random().nextInt(copyRunLengths[j]); // generate random progress rate for each reducer

                        progressScore = progress / copyRunLengths[j];
                        specDec = ((1 - progressScore) / progressRate);

                        if (specDec < specThres) {
                            mrNodes[i].deleteReducer(runningReducer);
                        } else {
                            speculateReducer(cluster, i, runningReducer);
                        }

                        // shuffle phase
                        progress = new Random().nextInt(shuffleRunLengths[j]); // generate random current progress
                        progressRate = new Random().nextInt(shuffleRunLengths[j]); // generate random progress rate for each reducer

                        progressScore = progress / shuffleRunLengths[j];
                        specDec = ((1 - progressScore) / progressRate);

                        if (specDec < specThres) {
                            mrNodes[i].deleteReducer(runningReducer);
                        } else {
                            speculateReducer(cluster, i, runningReducer);
                        }

                        // reduce phase
                        progress = new Random().nextInt(reduceRunLengths[j]); // generate random current progress
                        progressRate = new Random().nextInt(reduceRunLengths[j]); // generate random progress rate for each reducer

                        progressScore = progress / reduceRunLengths[j];
                        specDec = ((1 - progressScore) / progressRate);

                        if (specDec < specThres) {
                            mrNodes[i].deleteReducer(runningReducer);
                        } else {
                            speculateReducer(cluster, i, runningReducer);
                        }

                    }
                }
            }

            // check if there is still reducers in any nodes
            numReducers = 0;
            for (int i = 0; i < Config.numNodes; i++) {
                numReducers += mrNodes[i].getMapSlot().size();
            }
        }
    }
}
