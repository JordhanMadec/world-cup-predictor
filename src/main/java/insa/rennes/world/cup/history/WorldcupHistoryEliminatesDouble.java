package insa.rennes.world.cup.history;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.util.Collector;

public class WorldcupHistoryEliminatesDouble implements GroupReduceFunction<
        Tuple5<String, Integer, Double, Double, Double>,
        Tuple5<String, Integer, Double, Double, Double>> {
    @Override
    public void reduce(
            Iterable<Tuple5<String, Integer, Double, Double, Double>> in,
            Collector<Tuple5<String, Integer, Double, Double, Double>> out
    ) throws Exception {
        double finalsRatio = 0.0;
        double semiFinalsRatio = 0.0;
        String country = "";
        int edition = 0;
        double hosting = 0.0;

        for(Tuple5<String, Integer, Double, Double, Double> tuple: in) {
            country = tuple.f0;
            edition = tuple.f1;
            finalsRatio += tuple.f2;
            semiFinalsRatio += tuple.f3;
            hosting = tuple.f4;
        }

        out.collect(new Tuple5(country, edition, finalsRatio, semiFinalsRatio, hosting));
    }
}
