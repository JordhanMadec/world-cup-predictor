package insa.rennes;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.util.Collector;

import java.util.Date;

public class FifaRankingStats implements FlatMapFunction<Tuple6<Integer, String, Float, Integer, Integer, Date>, Tuple3<String, Integer, Integer>> {
    @Override
    public void flatMap(Tuple6<Integer, String, Float, Integer, Integer, Date> in, Collector<Tuple3<String, Integer, Integer>> out) throws Exception {
        // (team, rank, number of values)
        out.collect(new Tuple3(in.f1, in.f0, 1));
    }
}