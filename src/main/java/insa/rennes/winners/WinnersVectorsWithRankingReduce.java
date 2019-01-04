package insa.rennes.winners;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple10;
import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.util.Collector;

public class WinnersVectorsWithRankingReduce implements GroupReduceFunction<
        Tuple10<String, Integer, Double, Integer, Double, Double, Double, Integer, Integer, Double>,
        Tuple8<Double, Double, Double, Double, Double, Double, Double, Double>> {

    @Override
    public void reduce(
            Iterable<Tuple10<String, Integer, Double, Integer, Double, Double, Double, Integer, Integer, Double>> in,
            Collector<Tuple8<Double, Double, Double, Double, Double, Double, Double, Double>> out
    ) throws Exception {

        double rank = 0.0;
        double rankEvolution = 0.0;
        double winRatio = 0.0;
        double lossRatio = 0.0;
        double goalsRatio = 0.0;
        double finalsPlayed = 0.0;
        double finalsWon = 0.0;
        double finalsRatio = 0.0;

        int nbWinners = 0;

        for (Tuple10<String, Integer, Double, Integer, Double, Double, Double, Integer, Integer, Double> tuple: in) {
            rank += tuple.f2;
            rankEvolution += tuple.f3;
            winRatio += tuple.f4;
            lossRatio += tuple.f5;
            goalsRatio += tuple.f6;
            finalsPlayed += tuple.f7;
            finalsWon += tuple.f8;
            finalsRatio += tuple.f9;

            nbWinners++;
        }

        rank /= nbWinners;
        rankEvolution /= nbWinners;
        winRatio /= nbWinners;
        lossRatio /= nbWinners;
        goalsRatio /= nbWinners;
        finalsPlayed /= nbWinners;
        finalsWon /= nbWinners;
        finalsRatio /= nbWinners;

        double norm = Math.sqrt(rank*rank + rankEvolution*rankEvolution + winRatio*winRatio + lossRatio*lossRatio + goalsRatio*goalsRatio + finalsPlayed*finalsPlayed + finalsWon*finalsWon + finalsRatio*finalsRatio);

        rank /= norm;
        rankEvolution /= norm;
        winRatio /= norm;
        lossRatio /= norm;
        goalsRatio /= norm;
        finalsPlayed /= norm;
        finalsWon /= norm;
        finalsRatio /= norm;

        // (rank average, rank evolution, win ratio, loss ratio, goals ratio, finals played, finals won, ratio)
        out.collect(new Tuple8(rank, rankEvolution, winRatio, lossRatio, goalsRatio, finalsPlayed, finalsWon, finalsRatio));
    }
}
