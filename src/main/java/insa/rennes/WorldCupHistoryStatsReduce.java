package insa.rennes;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.util.Collector;

public class WorldCupHistoryStatsReduce implements GroupReduceFunction<Tuple3<String, Integer, Integer>, Tuple4<String, Integer, Integer, Double>> {

    @Override
    public void reduce(Iterable<Tuple3<String, Integer, Integer>> in, Collector<Tuple4<String, Integer, Integer, Double>> out) {
        int finalsPlayed = 0;
        int finalsWon = 0;
        String country = "";

        for(Tuple3<String, Integer, Integer> tuple: in) {
            country = tuple.f0;
            finalsPlayed += tuple.f1;
            finalsWon += tuple.f2;
        }

        // (team, finals played, finals won, ratio)
        out.collect(new Tuple4(country, finalsPlayed, finalsWon, finalsWon * 1.0 / finalsPlayed));
    }
}
