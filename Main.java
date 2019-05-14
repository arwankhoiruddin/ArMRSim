import config.Config;
import hardware.*;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Map;
import java.util.Random;

import mapreduce.*;
import org.apache.commons.math3.distribution.ZipfDistribution;

import javax.swing.plaf.synth.SynthCheckBoxUI;
import javax.xml.crypto.Data;

public class Main {

    public static void main(String[] args) {

        // randomize the users data
        int[] data = Config.generateRandomDataSize();

        // generate the nodes
        Cluster cluster = new Cluster();

        // distribute the data blocks
        cluster.distributeData(data);

        // generate the jobs. One user one job
        Mapper[] mappers = new Mapper[Config.numUsers];
        for (int i=0; i<mappers.length; i++) {
            mappers[i] = new Mapper(i);
        }

        // schedule the mapper
        Scheduler.scheduleMap(cluster, mappers);

        // run mapper and speculate when needed
        Scheduler.runMapper(cluster);

        System.out.println("mapper finished");

        // check the intermediaries on each nodes
        MRNode[] nodes = cluster.getNodes();

        // schedule the reducer
        Scheduler.scheduleReducer(cluster);

        for (int i=0; i<Config.numNodes; i++) {
            System.out.println("Node number: " + i + " has reduce slot: " + nodes[i].getReduceSlot().size());
        }

        // run reducers phase and speculate when needed
        Scheduler.runReducer(cluster);

        System.out.println("Finished running");
    }
}