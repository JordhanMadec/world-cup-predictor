package insa.rennes.winners;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.api.java.tuple.Tuple9;
import org.apache.flink.util.Collector;

public class WinnerReduce implements GroupReduceFunction<
        Tuple9<String, Integer, Double, Double, Double, Double, Double, Double, Double>,
        Tuple7<Double, Double, Double, Double, Double, Double, Double>> {

    @Override
    public void reduce(
            Iterable<Tuple9<String, Integer, Double, Double, Double, Double, Double, Double, Double>> in,
            Collector<Tuple7<Double, Double, Double, Double, Double, Double, Double>> out
    ) throws Exception {

        double rankWeight = 0.0;
        double winRatio = 0.0;
        double lossRatio = 0.0;
        double goalsRatio = 0.0;
        double finalsRatio = 0.0;
        double semiFinalsRatio = 0.0;
        double hosting = 0.0;

        int nbWinners = 0;

        for (Tuple9<String, Integer, Double, Double, Double, Double, Double, Double, Double> tuple: in) {
            rankWeight += tuple.f2;
            winRatio += tuple.f3;
            lossRatio += tuple.f4;
            goalsRatio += tuple.f5;
            finalsRatio += tuple.f6;
            semiFinalsRatio += tuple.f7;
            hosting += tuple.f8;

            nbWinners++;
        }

        rankWeight /= nbWinners;
        winRatio /= nbWinners;
        lossRatio /= nbWinners;
        goalsRatio /= nbWinners;
        finalsRatio /= nbWinners;
        semiFinalsRatio /= nbWinners;
        hosting /= nbWinners;

        // (rank average, rank evolution, win ratio, loss ratio, goals ratio, finals ratio, semi finals ratio, hosting)
        out.collect(new Tuple7(rankWeight, winRatio, lossRatio, goalsRatio, finalsRatio, semiFinalsRatio, hosting));
    }
}
