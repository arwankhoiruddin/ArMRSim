import config.Config;
import hardware.*;

import java.util.ArrayList;
import java.util.Random;

import mapreduce.Mapper;
import mapreduce.Scheduler;
import org.apache.commons.math3.distribution.ZipfDistribution;

public class Main {

    // define simulation variables here

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

        // schedule the reducer
        Scheduler.runReducer(cluster);
        // run reducers phase and speculate when needed

        System.out.println("Finished running");
    }
}