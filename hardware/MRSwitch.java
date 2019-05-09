package hardware;

import java.util.Random;

public class MRSwitch {
    private int switchNumber;
    private int routerNumber;
    private int latency; // in micro seconds

    public MRSwitch(int switchNumber, int routerNumber) {
        this.switchNumber = switchNumber;
        this.routerNumber = routerNumber;
        this.latency = new Random().nextInt(10000);

    }

    public int getSwitchNumber() {
        return this.switchNumber;
    }

    public int getRouterNumber() {
        return this.routerNumber;
    }

    public int getLatency() { return new Random().nextInt(10000); }

}