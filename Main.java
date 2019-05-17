import config.Config;
import hardware.*;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Map;
import java.util.Random;

import mapreduce.*;
import static config.Config.cluster;

public class Main {

    public static void main(String[] args) {

        // randomize the users data
        int[] data = Config.generateRandomDataSize();

        // initialize the cluster
        cluster = new Cluster();

        // distribute the data blocks
        cluster.distributeData(data);

        // generate the jobs. For now I create one user one job
        Mapper[] mappers = new Mapper[Config.numUsers];
        for (int i=0; i<mappers.length; i++) {
            mappers[i] = new Mapper(i);
            Cluster.mapperQueue.add(mappers[i]);
        }

        // schedule and run mapper
        Scheduler.runScheduleMapper(cluster);

        System.out.println("===============");
        System.out.println("Running reducer");

        // schedule and run reducer
        Scheduler.runScheduleReducer(cluster);

        System.out.println("Finished running");
    }
}