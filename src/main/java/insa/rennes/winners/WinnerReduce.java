package insa.rennes.winners;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.api.java.tuple.Tuple10;
import org.apache.flink.util.Collector;

public class WinnerReduce implements GroupReduceFunction<
        Tuple10<String, Integer, Double, Double, Double, Double, Double, Double, Double, Double>,
        Tuple8<Double, Double, Double, Double, Double, Double, Double, Double>> {

    @Override
    public void reduce(
            Iterable<Tuple10<String, Integer, Double, Double, Double, Double, Double, Double, Double, Double>> in,
            Collector<Tuple8<Double, Double, Double, Double, Double, Double, Double, Double>> out
    ) throws Exception {

        double rankWeight = 0.0;
        double winRatio = 0.0;
        double lossRatio = 0.0;
        double goalsRatio = 0.0;
        double goalsPerMatch = 0.0;
        double finalsRatio = 0.0;
        double semiFinalsRatio = 0.0;
        double hosting = 0.0;

        int nbWinners = 0;

        for (Tuple10<String, Integer, Double, Double, Double, Double, Double, Double, Double, Double> tuple: in) {
            rankWeight += tuple.f2;
            winRatio += tuple.f3;
            lossRatio += tuple.f4;
            goalsRatio += tuple.f5;
            goalsPerMatch += tuple.f6;
            finalsRatio += tuple.f7;
            semiFinalsRatio += tuple.f8;
            hosting += tuple.f9;

            nbWinners++;
        }

        rankWeight /= nbWinners;
        winRatio /= nbWinners;
        lossRatio /= nbWinners;
        goalsRatio /= nbWinners;
        goalsPerMatch /= nbWinners;
        finalsRatio /= nbWinners;
        semiFinalsRatio /= nbWinners;
        hosting /= nbWinners;

        // (rank average, rank evolution, win ratio, loss ratio, goals ratio, finals ratio, semi finals ratio, hosting)
        out.collect(new Tuple8(rankWeight, winRatio, lossRatio, goalsRatio, goalsPerMatch, finalsRatio, semiFinalsRatio, hosting));
    }
}
