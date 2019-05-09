import org.apache.commons.math3.distribution.ZipfDistribution;

import java.util.ArrayList;

public class Coba {
    public static void main(String[] args) {
        ArrayList<Integer> a = new ArrayList<>();
        a.add(3);
        a.add(6);
        a.add(2);
        a.add(5);

        while (a.size() > 0) {
            int min = 1000;
            for (int b:a) {
                if (b < min)
                    min = b;
            }
            System.out.println("minimum: " + min);
            System.out.println("index of min: " + a.indexOf((Integer) min));
            a.remove((Integer) min);
        }
    }
}
