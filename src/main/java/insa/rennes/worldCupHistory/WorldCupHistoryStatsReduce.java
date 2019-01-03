package insa.rennes;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.util.Collector;

public class WorldCupHistoryStatsReduce implements GroupReduceFunction<Tuple4<String, Integer, Integer, Integer>, Tuple5<String, Integer, Integer, Integer, Double>> {

    @Override
    public void reduce(Iterable<Tuple4<String, Integer, Integer, Integer>> in, Collector<Tuple5<String, Integer, Integer, Integer, Double>> out) {
        int finalsPlayed = 0;
        int finalsWon = 0;
        double ratio = 0.0;
        String country = "";

        for(Tuple4<String, Integer, Integer, Integer> tuple: in) {
            // (team, edition, finals played, finals won, ratio)
            out.collect(new Tuple5(country, Utils.getWorldCupEdition(tuple.f1), finalsPlayed, finalsWon, ratio));

            country = tuple.f0;
            finalsPlayed += tuple.f2;
            finalsWon += tuple.f3;
            ratio = finalsWon * 1.0 / finalsPlayed;

            // (team, edition, finals played, finals won, ratio)
            out.collect(new Tuple5(country, Utils.getWorldCupEdition(tuple.f1 + 1), finalsPlayed, finalsWon, ratio));
        }
    }
}
