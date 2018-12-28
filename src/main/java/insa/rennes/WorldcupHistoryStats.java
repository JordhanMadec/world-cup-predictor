package insa.rennes;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple10;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;

public class WorldcupHistoryStats implements FlatMapFunction<Tuple10<Integer, String, String, String, String, String, Integer, Integer, Integer, Float>, Tuple3<String, Integer,  Integer>> {
    @Override
    public void flatMap(Tuple10<Integer, String, String, String, String, String, Integer, Integer, Integer, Float> in, Collector<Tuple3<String, Integer, Integer>> out) throws Exception {
        // (team, finals played, finals won)
        out.collect(new Tuple3(in.f2, 1, 1));
        out.collect(new Tuple3(in.f3, 1, 0));
    }
}