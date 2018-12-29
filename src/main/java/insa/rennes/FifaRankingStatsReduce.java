package insa.rennes;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.shaded.guava18.com.google.common.collect.Iterables;
import org.apache.flink.util.Collector;

import java.util.Date;

public class FifaRankingStatsReduce implements GroupReduceFunction<Tuple5<String, Integer, Date, Integer, Integer>, Tuple4<String, Integer, Double, Integer>> {

    @Override
    public void reduce(Iterable<Tuple5<String, Integer, Date, Integer, Integer>> in, Collector<Tuple4<String, Integer, Double, Integer>> out) {

        // (team, edition, date, rank average, rank evolution)

        double rankSum = 0.0;
        int count = 0;
        int edition = 0;
        String country = "";

        Tuple5<String, Integer, Date, Integer, Integer> start = null;
        Tuple5<String, Integer, Date, Integer, Integer> end = null;

        for(Tuple5<String, Integer, Date, Integer, Integer> tuple: in) {
            country = tuple.f0;
            edition = tuple.f1;
            rankSum += tuple.f3;
            count += tuple.f4;

            if (start == null || tuple.f2.compareTo(start.f2) < 0) {
                start = tuple;
            }

            if (end == null || tuple.f2.compareTo(end.f2) > 0) {
                end = tuple;
            }
        }

        int rankEvolution = start.f3 - end.f3;

        out.collect(new Tuple4(country, edition, rankSum/count, rankEvolution));
    }

}
