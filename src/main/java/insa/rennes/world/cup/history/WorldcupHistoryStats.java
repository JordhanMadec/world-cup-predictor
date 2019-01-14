package insa.rennes.world.cup.history;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple10;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.util.Collector;

public class WorldcupHistoryStats implements FlatMapFunction<Tuple10<Integer, String, String, String, String, String, Integer, Integer, Integer, Float>, Tuple5<String, Integer, Integer,  Integer, Integer>> {
    @Override
    public void flatMap(Tuple10<Integer, String, String, String, String, String, Integer, Integer, Integer, Float> in, Collector<Tuple5<String, Integer, Integer, Integer, Integer>> out) throws Exception {
        // (team, edition, finals played, finals won, semi finals, hosting country)

        out.collect(new Tuple5(in.f2, in.f0, 1, 1, 1));
        out.collect(new Tuple5(in.f3, in.f0, 1, 0, 1));
        out.collect(new Tuple5(in.f4, in.f0, 0, 0, 1));
        out.collect(new Tuple5(in.f5, in.f0, 0, 0, 1));
    }
}