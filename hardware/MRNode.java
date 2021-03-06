package hardware;

import config.*;
import mapreduce.*;

import java.util.ArrayList;
import java.util.Random;

public class MRNode {
    // initializations
    private double CPUPower;
    private double RAM;
    private MRSwitch mrSwitch;

    private int occupancyTime;

    private ArrayList<DataBlock> dataBlocks = new ArrayList<>();

    private ArrayList<Mapper> mapSlot = new ArrayList<>();
    private ArrayList<Reducer> reduceSlot = new ArrayList<>();

    private ArrayList<Intermediary> intermediaries = new ArrayList<>();

    public MRNode(double CPUPower, double RAM, MRSwitch mrSwitch) {
        this.CPUPower = CPUPower;
        this.RAM = RAM;
        this.mrSwitch = mrSwitch;
    }

    public boolean hasMapJob() {
        if (mapSlot.size() > 0)
            return true;
        else
            return false;
    }

    public boolean hasReduceJob() {
        if (reduceSlot.size() > 0)
            return true;
        else
            return false;
    }

    public void addBlock(DataBlock block) {
        dataBlocks.add(block);
    }

    public boolean hasBlockNeeded(int taskID) {
        for (DataBlock block: dataBlocks) {
            if (block.getBlockNumber() == taskID) {
                return true;
            }
        }
        return false;
    }

    public MRSwitch getMrSwitch() {
        return this.mrSwitch;
    }

    public boolean hasMapSlot() {
        if (this.mapSlot.size() < Config.numMapSlots)
            return true;
        else
            return false;
    }

    public double runTask(MRTask task) {
        double duration = 0;

        // generate the additional task length due to node noise (e.g. CPU is busy with other tasks)
        int noise = new Random().nextInt(100);

        duration = noise + (task.length / this.CPUPower);
        return duration;
    }

    public boolean hasReduceSlot() {
        if (reduceSlot.size() < Config.numReduceSlots)
            return true;
        else
            return false;
    }

    public void addMap(Mapper m) {
        if (hasMapSlot())
            this.mapSlot.add(m);
    }

    public void addIntermediary(Intermediary intermediary) {
        this.intermediaries.add(intermediary);
    }

    public void addReduce(Reducer r) {
        if (hasReduceSlot())
            reduceSlot.add(r);
    }

    public ArrayList<Mapper> getMapSlot() {
        return this.mapSlot;
    }

    public ArrayList<Reducer> getReduceSlot() {
        return this.reduceSlot;
    }

    public ArrayList<Intermediary> getIntermediaries() {
        return this.intermediaries;
    }

    public ArrayList<DataBlock> getDataBlocks() {
        return this.dataBlocks;
    }

    public void deleteMapper(Mapper mapper) {
        this.mapSlot.remove(mapper);
    }

    public void deleteReducer(Reducer reducer) {
        this.reduceSlot.remove(reducer);
    }
}