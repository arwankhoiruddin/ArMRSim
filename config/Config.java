package config;

import mapreduce.MapScheduler;
import mapreduce.ReduceScheduler;

import java.util.Random;

public class Config {
    public static int numUsers = 10;

    public static int blockSize = 64; // in megabytes
    public static int dataReplication = 3;

    public static int numMapSlots = 9;
    public static int numReduceSlots = 4;

    public static int numNodes = 10; // the network structure here will use tree topology
    public static boolean homogenous = true;

    public static int RAM = 4096;

    public static MapScheduler mapScheduler = MapScheduler.FIFO;
    public static ReduceScheduler reduceScheduler = ReduceScheduler.LBBS;

    public static int[] generateRandomDataSize() {

        int[] data = new int[Config.numUsers];

        for (int i = 0; i < Config.numUsers; i++) {
            data[i] = (10 + new Random().nextInt(100)); // some random megs data
        }
        return data;
    }

}