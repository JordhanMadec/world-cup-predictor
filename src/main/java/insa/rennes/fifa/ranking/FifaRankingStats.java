package insa.rennes.fifa.ranking;

import insa.rennes.Utils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.util.Collector;

import java.time.ZoneId;
import java.util.Date;

public class FifaRankingStats implements FlatMapFunction<Tuple6<Integer, String, Float, Integer, Integer, Date>, Tuple5<String, Integer, Date, Integer, Integer>> {
    @Override
    public void flatMap(Tuple6<Integer, String, Float, Integer, Integer, Date> in, Collector<Tuple5<String, Integer, Date, Integer, Integer>> out) throws Exception {
        // (team, edition, date, rank, number of values)
        int year = in.f5.toInstant().atZone(ZoneId.systemDefault()).toLocalDate().getYear();
        int edition = Utils.getWorldCupEdition(year);

        if (edition > 0) {
            out.collect(new Tuple5(in.f1, edition, in.f5, in.f0, 1));
        }
    }
}