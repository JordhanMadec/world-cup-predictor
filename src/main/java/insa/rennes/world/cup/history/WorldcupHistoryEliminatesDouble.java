package insa.rennes.world.cup.history;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.util.Collector;

public class WorldcupHistoryEliminatesDouble implements GroupReduceFunction<
        Tuple5<String, Integer, Integer, Integer, Double>,
        Tuple5<String, Integer, Integer, Integer, Double>> {
    @Override
    public void reduce(
            Iterable<Tuple5<String, Integer, Integer, Integer, Double>> in,
            Collector<Tuple5<String, Integer, Integer, Integer, Double>> out
    ) throws Exception {
        int finalsPlayed = 0;
        int finalsWon = 0;
        double ratio = 0.0;
        String country = "";
        int edition = 0;

        for(Tuple5<String, Integer, Integer, Integer, Double> tuple: in) {
            country = tuple.f0;
            edition = tuple.f1;
            finalsPlayed += tuple.f2;
            finalsWon += tuple.f3;
            ratio += tuple.f4;
        }

        out.collect(new Tuple5(country, edition, finalsPlayed, finalsWon, ratio));
    }
}
