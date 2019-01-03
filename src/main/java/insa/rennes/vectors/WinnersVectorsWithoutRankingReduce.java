package insa.rennes.vectors;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.util.Collector;

public class WinnersVectorsWithoutRankingReduce implements GroupReduceFunction<
        Tuple8<String, Integer, Double, Double, Double, Integer, Integer, Double>,
        Tuple6<Double, Double, Double, Double, Double, Double>> {

    @Override
    public void reduce(
            Iterable<Tuple8<String, Integer, Double, Double, Double, Integer, Integer, Double>> in,
            Collector<Tuple6<Double, Double, Double, Double, Double, Double>> out
    ) throws Exception {

        double winRatio = 0.0;
        double lossRatio = 0.0;
        double goalsRatio = 0.0;
        double finalsPlayed = 0.0;
        double finalsWon = 0.0;
        double finalsRatio = 0.0;

        int nbWinners = 0;

        for (Tuple8<String, Integer, Double, Double, Double, Integer, Integer, Double> tuple: in) {
            winRatio += tuple.f2;
            lossRatio += tuple.f3;
            goalsRatio += tuple.f4;
            finalsPlayed += tuple.f5;
            finalsWon += tuple.f6;
            finalsRatio += tuple.f7;

            nbWinners++;
        }

        winRatio /= nbWinners;
        lossRatio /= nbWinners;
        goalsRatio /= nbWinners;
        finalsPlayed /= nbWinners;
        finalsWon /= nbWinners;
        finalsRatio /= nbWinners;

        // (rank average, rank evolution, win ratio, loss ratio, goals ratio, finals played, finals won, ratio)
        out.collect(new Tuple6(winRatio, lossRatio, goalsRatio, finalsPlayed, finalsWon, finalsRatio));
    }
}
