package config;

import mapreduce.MapScheduler;

import java.util.Random;

public class Config {
    public static int numUsers = 10;

    public static int blockSize = 64; // in megabytes
    public static int dataReplication = 3;

    public static int numMapSlots = 3;
    public static int numReduceSlots = 1;

    public static int numNodes = 10; // the network structure here will use tree topology
    public static boolean homogenous = true;

    public static int RAM = 4096;

    public static MapScheduler mapScheduler = MapScheduler.FIFO;

    public static int[] generateRandomDataSize() {

        int[] data = new int[Config.numUsers];

        for (int i = 0; i < Config.numUsers; i++) {
            data[i] = (10 + new Random().nextInt(1000)) * 1024; // some random gigs data
        }
        return data;
    }

}