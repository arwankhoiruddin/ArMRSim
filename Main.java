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

        // print the histories
        for (int i=0; i<Cluster.histories.size(); i++) {
            History h = (History) Cluster.histories.get(i);
            Mapper m = (Mapper) h.getTask();
            System.out.println(m.getTaskType() + " with ID: " + m.getTaskID() + " and length of: " + m.getLength());
        }
//
//        // schedule the reducer
//        Scheduler.scheduleReducer(cluster);
//
//        Scheduler.runReducer(cluster);
//        // run reducers phase and speculate when needed
////
//        System.out.println("Finished running");
    }
}