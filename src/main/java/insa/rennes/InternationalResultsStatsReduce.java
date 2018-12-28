package insa.rennes;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple11;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;

public class InternationalResultsStatsReduce implements GroupReduceFunction<
    Tuple11<String, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>,
    Tuple3<String, Double, Double>
> {

    @Override
    public void reduce(
        Iterable<Tuple11<String, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>> iterable,
        Collector<Tuple3<String, Double, Double>> out
    ) throws Exception {

        // (country, win ratio, goals ratio)

        String team = "";
        double win = 0.0;
        double draw = 0.0;
        double loss = 0.0;
        double goals_for = 0.0;
        double goals_against = 0.0;

        for(Tuple11<String, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer> tuple: iterable) {
            team = tuple.f0;
            win += tuple.f1 * 1.0;
            draw += tuple.f2 * 1.0;
            loss += tuple.f3 * 1.0;
            goals_for += tuple.f4 * 1.0;
            goals_against += tuple.f5 * 1.0;
        }

        out.collect(new Tuple3(team, win / (win + draw + loss), goals_for / (goals_for + goals_against)));
    }
}
