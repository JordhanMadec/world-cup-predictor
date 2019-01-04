package insa.rennes.worldCupHistory;

import insa.rennes.Utils;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

public class WorldCupHistoryStatsReduce implements GroupReduceFunction<Tuple4<String, Integer, Integer, Integer>, Tuple5<String, Integer, Integer, Integer, Double>> {

    @Override
    public void reduce(Iterable<Tuple4<String, Integer, Integer, Integer>> in, Collector<Tuple5<String, Integer, Integer, Integer, Double>> out) {
        int finalsPlayed = 0;
        int finalsWon = 0;
        double ratio = 0.0;
        String country = "";

        List<Integer> visited_editions = new ArrayList<Integer>();
        List<String> visited_countries = new ArrayList<String>();

        for(Tuple4<String, Integer, Integer, Integer> tuple: in) {
            country = tuple.f0;

            for(int edition: Utils.getEDITIONS()) {
                if (!visited_editions.contains(edition) && edition <= Utils.getWorldCupEdition(tuple.f1)) {
                    visited_editions.add(edition);
                    // (team, edition, finals played, finals won, ratio)
                    out.collect(new Tuple5(country, edition, finalsPlayed, finalsWon, ratio));
                }
            }

            finalsPlayed += tuple.f2;
            finalsWon += tuple.f3;
            ratio = finalsWon * 1.0 / finalsPlayed;

            // (team, edition, finals played, finals won, ratio)
            out.collect(new Tuple5(country, Utils.getWorldCupEdition(tuple.f1 + 1), finalsPlayed, finalsWon, ratio));
            visited_editions.add(Utils.getWorldCupEdition(tuple.f1 + 1));
        }

        if (visited_editions.size() < Utils.getEDITIONS().length) {
            for(int edition: Utils.getEDITIONS()) {
                if (!visited_editions.contains(edition) && edition > visited_editions.get(visited_editions.size() - 1)) {
                    visited_editions.add(edition);
                    out.collect(new Tuple5(country, edition, finalsPlayed, finalsWon, ratio));
                }
            }
        }

        for(String _country: Utils.getAllCountries()) {
            if (!_country.equals(country))
                for(int edition: Utils.getEDITIONS())
                    out.collect(new Tuple5(_country, edition, 0, 0, 0.0));
        }
    }
}
