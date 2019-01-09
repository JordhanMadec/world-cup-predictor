package insa.rennes.winners;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.util.Collector;

public class WinnerReduce implements GroupReduceFunction<
        Tuple8<String, Integer, Double, Double, Double, Double, Double, Double>,
        Tuple6<Double, Double, Double, Double, Double, Double>> {

    @Override
    public void reduce(
            Iterable<Tuple8<String, Integer, Double, Double, Double, Double, Double, Double>> in,
            Collector<Tuple6<Double, Double, Double, Double, Double, Double>> out
    ) throws Exception {

        double rankWeight = 0.0;
        double winRatio = 0.0;
        double lossRatio = 0.0;
        double goalsRatio = 0.0;
        double finalsRatio = 0.0;
        double semiFinalsRatio = 0.0;

        int nbWinners = 0;

        for (Tuple8<String, Integer, Double, Double, Double, Double, Double, Double> tuple: in) {
            rankWeight += tuple.f2;
            winRatio += tuple.f3;
            lossRatio += tuple.f4;
            goalsRatio += tuple.f5;
            finalsRatio += tuple.f6;
            semiFinalsRatio += tuple.f7;

            nbWinners++;
        }

        rankWeight /= nbWinners;
        winRatio /= nbWinners;
        lossRatio /= nbWinners;
        goalsRatio /= nbWinners;
        finalsRatio /= nbWinners;
        semiFinalsRatio /= nbWinners;

        // (rank average, rank evolution, win ratio, loss ratio, goals ratio, finals ratio, semi finals ratio)
        out.collect(new Tuple6(rankWeight, winRatio, lossRatio, goalsRatio, finalsRatio, semiFinalsRatio));
    }
}
