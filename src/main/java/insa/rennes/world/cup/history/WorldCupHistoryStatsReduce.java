package insa.rennes.world.cup.history;

import insa.rennes.Utils;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

public class WorldCupHistoryStatsReduce implements GroupReduceFunction<Tuple5<String, Integer, Integer, Integer, Integer>, Tuple5<String, Integer, Double, Double, Double>> {

    @Override
    public void reduce(Iterable<Tuple5<String, Integer, Integer, Integer, Integer>> in, Collector<Tuple5<String, Integer, Double, Double, Double>> out) {
        int finalsPlayed = 0;
        int finalsWon = 0;
        int semiFinals = 0;
        double semiFinalsRatio = 0.0;
        double finalsRatio = 0.0;
        String country = "";
        double nbEdition = 0.0;
        double hosting = 0.0;

        List<Integer> visited_editions = new ArrayList<Integer>();

        for(Tuple5<String, Integer, Integer, Integer, Integer> tuple: in) {
            country = tuple.f0;

            for(int edition: Utils.getEDITIONS()) {
                if (!visited_editions.contains(edition) && edition <= tuple.f1) {
                    visited_editions.add(edition);
                    nbEdition++;
                    semiFinalsRatio = semiFinals * 1.0 / nbEdition;
                    hosting = (country.compareTo(Utils.getHost(edition)) == 0) ? 1.0 : 0.0;

                    // (team, edition, finals ratio, semi finals ratio, hosting)
                    out.collect(new Tuple5(country, edition, finalsRatio, semiFinalsRatio, hosting));
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
                if ((!visited_editions.contains(edition) && edition > visited_editions.get(visited_editions.size() - 1))) {
                    visited_editions.add(edition);
                    nbEdition++;
                    semiFinalsRatio = semiFinals * 1.0 / nbEdition;
                    hosting = (country.compareTo(Utils.getHost(edition)) == 0) ? 1.0 : 0.0;

                    // (team, edition, finals ratio, semi finals ratio)
                    out.collect(new Tuple5(country, edition, finalsRatio, semiFinalsRatio, hosting));
                }
            }
        }

        for(String _country: Utils.getAllCountries()) {
            if (!_country.equals(country)) {
                for (int edition : Utils.getEDITIONS()) {
                    hosting = (_country.compareTo(Utils.getHost(edition)) == 0) ? 1.0 : 0.0;
                    out.collect(new Tuple5(_country, edition, 0.0, 0.0, hosting));
                }
            }
        }
    }
}
