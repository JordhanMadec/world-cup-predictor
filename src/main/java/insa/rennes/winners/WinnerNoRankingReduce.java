package insa.rennes.winners;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.util.Collector;

public class WinnerNoRankingReduce implements GroupReduceFunction<
        Tuple6<String, Integer, Double, Double, Double, Double>,
        Tuple4<Double, Double, Double, Double>> {

    @Override
    public void reduce(
            Iterable<Tuple6<String, Integer, Double, Double, Double, Double>> in,
            Collector<Tuple4<Double, Double, Double, Double>> out
    ) throws Exception {

        double winRatio = 0.0;
        double lossRatio = 0.0;
        double goalsRatio = 0.0;
        double finalsRatio = 0.0;

        int nbWinners = 0;

        for (Tuple6<String, Integer, Double, Double, Double, Double> tuple: in) {
            winRatio += tuple.f2;
            lossRatio += tuple.f3;
            goalsRatio += tuple.f4;
            finalsRatio += tuple.f5;

            nbWinners++;
        }

        winRatio /= nbWinners;
        lossRatio /= nbWinners;
        goalsRatio /= nbWinners;
        finalsRatio /= nbWinners;

        // (rank average, rank evolution, win ratio, loss ratio, goals ratio, finals ratio)
        out.collect(new Tuple4(winRatio, lossRatio, goalsRatio, finalsRatio));
    }
}
