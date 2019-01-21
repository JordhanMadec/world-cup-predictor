package insa.rennes.international.results;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.util.Collector;

public class InternationalResultsStatsReduce implements GroupReduceFunction<
    Tuple7<String, Integer, Integer, Integer, Integer, Integer, Integer>,
    Tuple6<String, Integer, Double, Double, Double, Double>> {

    @Override
    public void reduce(
        Iterable<Tuple7<String, Integer, Integer, Integer, Integer, Integer, Integer>> iterable,
        Collector<Tuple6<String, Integer, Double, Double, Double, Double>> out
    ) throws Exception {

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

        // (team, edition, win ratio, loss ratio, goals ratio, goals per match)
        out.collect(new Tuple6(team, year, win / (win + draw + loss), loss / (win + draw + loss), goals_for / (goals_for + goals_against), goals_for / (win + draw + loss)));
    }
}
