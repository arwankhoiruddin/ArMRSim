package mapreduce;

import config.Config;
import hardware.Cluster;
import hardware.MRNode;
import org.apache.commons.math3.distribution.ZipfDistribution;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Random;

public class Scheduler {

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

    public static void runMapper(Cluster cluster) {
        System.out.println("Start running mapper");
        // the length of mapper is using Zipf distribution
        MRNode[] mrNodes = cluster.getNodes();
        int maxLengthRun = 1000;

        // speculation threshold
        double specThres = 0.7;

        // keep running while there is still mappers in any nodes
        int numMappers = 100000;

        while (numMappers != 0) {

            for (int i=0; i<mrNodes.length; i++) {

                ZipfDistribution zipfDistribution = new ZipfDistribution(maxLengthRun, 0.5);
                if (mrNodes[i].getMapSlot().size() > 0) {
                    int[] tmp = zipfDistribution.sample(mrNodes[i].getMapSlot().size());
                    ArrayList<Integer> mapRunLengths = new ArrayList<Integer>();
                    for (int t : tmp) {
                        mapRunLengths.add(t);
                    }

                    int j = 0;
                    while (true) {
                        System.out.println("Running mapper");

                        if (mrNodes[i].getMapSlot().size() == 0) break;
                        if (j == Config.numUsers) break;

                        System.out.println("size of the mapslot: " + mrNodes[i].getMapSlot().size());
                        ArrayList<Mapper> mappers = mrNodes[i].getMapSlot();

                        Mapper runningMapper = mappers.get(0); // run from the first mapper, cause this is FIFO

                        // only run if the mapper is in the same node with the data block
                        if (mrNodes[i].hasBlockOfUser(runningMapper.getUserID())) {
                            System.out.println("in node");
                            int progress = new Random().nextInt(mapRunLengths.get(0)); // generate random current progress
                            int progressRate = new Random().nextInt(mapRunLengths.get(0)); // generate random progress rate for each mapper

                            // LATE algorithm
                            double progressScore = progress / mapRunLengths.get(0);
                            double specDec = ((1 - progressScore) / progressRate);

                            if (specDec < specThres) {
                                mrNodes[i].deleteMapper(runningMapper);
                                mapRunLengths.remove(0);
                            } else {
                                speculateMapper(cluster, i, runningMapper);
                                mrNodes[i].deleteMapper(runningMapper);
                                mapRunLengths.remove(0);
                            }
                        } else { // the block is not in the current node. find the node containing the block needed
                            System.out.println("not in node");
                            for (int node=0; node<mrNodes.length; node++) {
                                if (mrNodes[node].hasBlockOfUser(runningMapper.getUserID())) {
                                    if (mrNodes[node].hasMapSlot()) {
                                        mrNodes[node].addMap(runningMapper);
                                        mrNodes[i].deleteMapper(runningMapper);
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
        }
    }

    public static Reducer[] scheduleReducer(Cluster cluster) {
        ArrayList<Reducer> reducers = new ArrayList<>();

        // LBBS Algorithm
        // randomly create intermediary results (<Key,Value> results from Map task). To avoid zero, I give 2 as minimum
        int intermediaryPartition = 2 + new Random().nextInt(100);


        int[][] partition = new int[intermediaryPartition][Config.numUsers];

        double[][] locality1 = new double[intermediaryPartition][Config.numNodes];
        double[][] locality2 = new double[intermediaryPartition][Config.numNodes];
        double[][] locality = new double[intermediaryPartition][Config.numNodes];

        for (int p=0; p<intermediaryPartition; p++) {
            for (int n=0; n<Config.numNodes; n++) {
                // randomize the partition
                partition[p][n] = new Random().nextInt(100);
            }
        }

        for (int p=0; p<intermediaryPartition; p++) {
            for (int n=0; n<Config.numNodes; n++) {
                int tmp = 0;
                for (int i=0; i<intermediaryPartition; i++) {
                    tmp += partition[i][n];
                }
                locality1[p][n] = partition[p][n] / tmp;

                tmp = 0;
                for (int i=0; i<Config.numNodes; i++) {
                    tmp += partition[p][i];
                }
                locality2[p][n] = partition[p][n] / tmp;
                locality[p][n] = locality1[p][n] * locality2[p][n];
            }
        }

        // find the workloads for each nodes
        int[] heap = new int[Config.numNodes];

        for (int n=0; n<Config.numNodes; n++) {
            int tmp = 0;
            for (int p=0; p<intermediaryPartition; p++) {
                tmp += partition[p][n];
            }
            heap[n] = tmp;
            System.out.println(heap[n]);
        }

        return (Reducer[]) reducers.toArray();
    }

    public static void runReducer(Cluster cluster, Reducer[] reducer) {

        // the length of reducer is using Zipf distribution
        MRNode[] mrNodes = cluster.getNodes();
        int maxLengthRun = 100000;

        // speculation threshold
        double specThres = 0.7;


        // keep running while there is still mappers in any nodes
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
