package insa.rennes.world.cup.history;

import insa.rennes.Utils;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

public class WorldCupHistoryStatsReduce implements GroupReduceFunction<Tuple5<String, Integer, Integer, Integer, Integer>, Tuple4<String, Integer, Double, Double>> {

    @Override
    public void reduce(Iterable<Tuple5<String, Integer, Integer, Integer, Integer>> in, Collector<Tuple4<String, Integer, Double, Double>> out) {
        int finalsPlayed = 0;
        int finalsWon = 0;
        int semiFinals = 0;
        double semiFinalsRatio = 0.0;
        double finalsRatio = 0.0;
        String country = "";
        double nbEdition = 0.0;

        List<Integer> visited_editions = new ArrayList<Integer>();

        for(Tuple5<String, Integer, Integer, Integer, Integer> tuple: in) {
            country = tuple.f0;

            for(int edition: Utils.getEDITIONS()) {
                if (!visited_editions.contains(edition) && edition <= Utils.getWorldCupEdition(tuple.f1)) {
                    visited_editions.add(edition);
                    nbEdition++;
                    semiFinalsRatio = semiFinals * 1.0 / nbEdition;

                    // (team, edition, finals ratio, semi finals ratio)
                    out.collect(new Tuple4(country, edition, finalsRatio, semiFinalsRatio));
                }
            }

            nbEdition++;
            finalsPlayed += tuple.f2;
            finalsWon += tuple.f3;
            finalsRatio = finalsPlayed > 0 ? finalsWon * 1.0 / finalsPlayed : 0.0;
            semiFinals += tuple.f4;
        }

        if (visited_editions.size() < Utils.getEDITIONS().length) {
            for(int edition: Utils.getEDITIONS()) {
                if (!visited_editions.contains(edition) && edition > visited_editions.get(visited_editions.size() - 1)) {
                    visited_editions.add(edition);
                    nbEdition++;
                    semiFinalsRatio = semiFinals * 1.0 / nbEdition;

                    // (team, edition, finals ratio, semi finals ratio)
                    out.collect(new Tuple4(country, edition, finalsRatio, semiFinalsRatio));
                }
            }
        }

        for(String _country: Utils.getAllCountries()) {
            if (!_country.equals(country))
                for(int edition: Utils.getEDITIONS())
                    out.collect(new Tuple4(_country, edition, 0.0, 0.0));
        }
    }
}
