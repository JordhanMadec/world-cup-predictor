package insa.rennes;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple10;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;

public class WorldcupHistoryStats implements FlatMapFunction<Tuple10<Integer, String, String, String, String, String, Integer, Integer, Integer, Integer>, Tuple3<String, String,  Integer>> {
    @Override
    public void flatMap(Tuple10<Integer, String, String, String, String, String, Integer, Integer, Integer, Integer> in, Collector<Tuple3<String, String, Integer>> out) throws Exception {
        out.collect(new Tuple3(in.f2, "vainqueur",  1));
    }
}