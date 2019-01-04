package insa.rennes.winners;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.util.Collector;

public class WinnerReduce implements GroupReduceFunction<
        Tuple8<String, Integer, Double, Integer, Double, Double, Double, Double>,
        Tuple6<Double, Double, Double, Double, Double, Double>> {

    @Override
    public void reduce(
            Iterable<Tuple8<String, Integer, Double, Integer, Double, Double, Double, Double>> in,
            Collector<Tuple6<Double, Double, Double, Double, Double, Double>> out
    ) throws Exception {

        double rank = 0.0;
        double rankEvolution = 0.0;
        double winRatio = 0.0;
        double lossRatio = 0.0;
        double goalsRatio = 0.0;
        double finalsRatio = 0.0;

        int nbWinners = 0;

        for (Tuple8<String, Integer, Double, Integer, Double, Double, Double, Double> tuple: in) {
            rank += tuple.f2;
            rankEvolution += tuple.f3;
            winRatio += tuple.f4;
            lossRatio += tuple.f5;
            goalsRatio += tuple.f6;
            finalsRatio += tuple.f7;

            nbWinners++;
        }

        rank /= nbWinners;
        rankEvolution /= nbWinners;
        winRatio /= nbWinners;
        lossRatio /= nbWinners;
        goalsRatio /= nbWinners;
        finalsRatio /= nbWinners;

        double norm = Math.sqrt(rank*rank + rankEvolution*rankEvolution + winRatio*winRatio + lossRatio*lossRatio + goalsRatio*goalsRatio + finalsRatio*finalsRatio);

        rank /= norm;
        rankEvolution /= norm;
        winRatio /= norm;
        lossRatio /= norm;
        goalsRatio /= norm;
        finalsRatio /= norm;

        // (rank average, rank evolution, win ratio, loss ratio, goals ratio, finals ratio)
        out.collect(new Tuple6(rank, rankEvolution, winRatio, lossRatio, goalsRatio, finalsRatio));
    }
}
