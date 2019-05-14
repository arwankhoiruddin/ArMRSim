package mapreduce;

import config.Config;
import hardware.Cluster;
import hardware.MRNode;
import org.apache.commons.math3.distribution.ZipfDistribution;

import java.util.*;

public class Scheduler {

    public static int duration;

    public static void scheduleMap(Cluster cluster, Mapper[] mappers) {

        switch (Config.mapScheduler) {
            case FIFO:
                FIFOScheduler(cluster, mappers);
                break;
        }
    }

    private static void FIFOScheduler(Cluster cluster, Mapper[] mappers) {
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

    private static void speculateMapper(Cluster cluster, int currentNode, Mapper mapper) {
        MRNode[] nodes = cluster.getNodes();

        int nodeNumber = 0;

        while (true) {
            if (nodeNumber == cluster.getNodes().length)
                nodeNumber = 0;

            if (nodeNumber != currentNode) {
                if (nodes[nodeNumber].hasMapSlot()) {
                    nodes[nodeNumber].addMap(mapper);
                    break;
                }
            }

            nodeNumber++;
        }
    }

    public static void runMapper(Cluster cluster) {
        System.out.println("Start running mapper");
        // the length of mapper is using Zipf distribution
        MRNode[] mrNodes = cluster.getNodes();
        int maxLengthRun = 1000;

        // speculation threshold
        double specThres = 0.7;

        // keep running while there is still mappers in any nodes

        int numMappers = Config.numUsers; // one user submit one map task

        while (numMappers != 0) {

            for (int i=0; i<mrNodes.length; i++) {

                System.out.println("Node number: " + i);

                ZipfDistribution zipfDistribution = new ZipfDistribution(maxLengthRun, 0.5);
                if (mrNodes[i].getMapSlot().size() > 0) {
                    System.out.println("Number of maps in node: " + mrNodes[i].getMapSlot().size());

                    ArrayList<Mapper> m = mrNodes[i].getMapSlot();
                    for (int map=0; map<m.size(); map++) {
                        System.out.println("Map ID: " + m.get(map).getTaskID());
                    }

                    // generate length of the mappers
                    int[] tmp = zipfDistribution.sample(numMappers);

                    ArrayList<Integer> mapRunLengths = new ArrayList<Integer>();
                    for (int t : tmp) {
                        mapRunLengths.add(t);
                    }

                    int j = 0;
                    ArrayList<Mapper> mappers = mrNodes[i].getMapSlot();

                    while (true) {

                        if (mrNodes[i].getMapSlot().size() == 0) break;
                        if (j == Config.numUsers) break;

                        Mapper runningMapper = mappers.get(0); // run from the first mapper, cause this is FIFO

                        // only run if the mapper is in the same node with the data block
                        if (mrNodes[i].hasBlockNeeded(runningMapper.getTaskID())) {
                            runningMapper.setLength(mapRunLengths.get(0));

                            int progress = new Random().nextInt(mapRunLengths.get(0)); // generate random current progress
                            int progressRate = new Random().nextInt(mapRunLengths.get(0)); // generate random progress rate for each mapper

                            // LATE algorithm
                            double progressScore = progress / mapRunLengths.get(0);
                            double specDec = ((1 - progressScore) / progressRate);

                            if (specDec < specThres) {
                                System.out.println("Running mapper ID: " + runningMapper.getTaskID());
                                // mapper will create intermediary results
                                int maxInterm = 20;

                                int numIntermediary = 1 + new Random().nextInt(maxInterm);
                                // generate the intermediary
                                for (int interm=0; interm < numIntermediary; interm++) {
                                    Intermediary intermediary = new Intermediary(new Random().nextInt(50));
                                    mrNodes[i].addIntermediary(intermediary);
                                }

                                // also don't forget to put it in history
                                Cluster.histories.add(new History(runningMapper, runningMapper.getLength()));

                                mrNodes[i].deleteMapper(runningMapper);
                                mapRunLengths.remove(0);
                            } else {
                                speculateMapper(cluster, i, runningMapper);
                                mrNodes[i].deleteMapper(runningMapper);
                                mapRunLengths.remove(0);
                            }
                        } else { // the block is not in the current node. find the node containing the block needed
                            System.out.println("Map ID speculated: " + runningMapper.getTaskID());

                            for (int node=0; node<mrNodes.length; node++) {
                                if (mrNodes[node].hasBlockNeeded(runningMapper.getTaskID())) {
                                    if (mrNodes[node].hasMapSlot()) {
                                        mrNodes[node].addMap(runningMapper);
                                        mrNodes[i].deleteMapper(runningMapper);
                                        break;
                                    }
                                }
                            }
                        }

                        j++;
                    }
                }
            }

            // check if there is still mappers in any nodes
            numMappers = 0;
            for (int i=0; i < Config.numNodes; i++) {
                numMappers += mrNodes[i].getMapSlot().size();
            }
            System.out.println("num mappers: " + numMappers);
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

    public static void copyJob(int sourceNode, int targetNode) {

    }

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

        switch (Config.reduceScheduler) {
            case LBBS:
                LBBSAlgorithm(allInterms, intermediaryPartition, nodes);
                break;
        }
    }

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

        System.out.println("Number of reducer: " + numReduce);

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
                nodes[heap.indexOf(min)].addReduce(new Reducer(new Random().nextInt(numReduce)));
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
                        Reducer runningReducer = reducers.get(mrNodes[i].getReduceSlot().size());

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

            // check if there is still mappers in any nodes
            numReducers = 0;
            for (int i = 0; i < Config.numNodes; i++) {
                numReducers += mrNodes[i].getMapSlot().size();
            }
        }
    }
}
