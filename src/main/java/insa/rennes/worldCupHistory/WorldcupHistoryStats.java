package insa.rennes.worldCupHistory;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple10;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.util.Collector;

public class WorldcupHistoryStats implements FlatMapFunction<Tuple10<Integer, String, String, String, String, String, Integer, Integer, Integer, Float>, Tuple4<String, Integer, Integer,  Integer>> {
    @Override
    public void flatMap(Tuple10<Integer, String, String, String, String, String, Integer, Integer, Integer, Float> in, Collector<Tuple4<String, Integer, Integer, Integer>> out) throws Exception {
        // (team, edition, finals played, finals won)

        out.collect(new Tuple4(in.f2, in.f0, 1, 1));
        out.collect(new Tuple4(in.f3, in.f0, 1, 0));
    }
}