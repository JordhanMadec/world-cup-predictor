package insa.rennes.world.cup.history;

import insa.rennes.Utils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple10;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class WorldcupWinners implements FlatMapFunction<Tuple10<Integer, String, String, String, String, String, Integer, Integer, Integer, Float>, Tuple2<String, Integer>> {
    @Override
    public void flatMap(Tuple10<Integer, String, String, String, String, String, Integer, Integer, Integer, Float> in, Collector<Tuple2<String, Integer>> out) throws Exception {
        // (team, edition, finals played, finals won)
        int edition = Utils.getWorldCupEdition(in.f0);

        out.collect(new Tuple2(in.f2, edition));
    }
}