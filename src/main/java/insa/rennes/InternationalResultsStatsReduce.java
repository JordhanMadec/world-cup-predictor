package insa.rennes;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.util.Collector;

public class InternationalResultsStatsReduce implements GroupReduceFunction<
    Tuple7<String, Integer, Integer, Integer, Integer, Integer, Integer>,
    Tuple5<String, Integer, Double, Double, Double>
> {

    @Override
    public void reduce(
        Iterable<Tuple7<String, Integer, Integer, Integer, Integer, Integer, Integer>> iterable,
        Collector<Tuple5<String, Integer, Double, Double, Double>> out
    ) throws Exception {

        // (country, win ratio, loss ratio, goals ratio)

        String team = "";
        int year = 0;
        double win = 0.0;
        double draw = 0.0;
        double loss = 0.0;
        double goals_for = 0.0;
        double goals_against = 0.0;

        for(Tuple7<String, Integer, Integer, Integer, Integer, Integer, Integer> tuple: iterable) {
            team = tuple.f0;
            year = tuple.f1;
            win += tuple.f2 * 1.0;
            draw += tuple.f3 * 1.0;
            loss += tuple.f4 * 1.0;
            goals_for += tuple.f5 * 1.0;
            goals_against += tuple.f6 * 1.0;
        }

        out.collect(new Tuple5(team, year, win / (win + draw + loss), loss / (win + draw + loss), goals_for / (goals_for + goals_against)));
    }
}
