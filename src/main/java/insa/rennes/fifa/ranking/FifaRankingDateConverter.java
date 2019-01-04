package insa.rennes.fifa.ranking;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.util.Collector;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

public class FifaRankingDateConverter implements FlatMapFunction<Tuple6<Integer, String, Float, Integer, Integer, String>, Tuple6<Integer, String, Float, Integer, Integer, Date>> {
    @Override
    public void flatMap(Tuple6<Integer, String, Float, Integer, Integer, String> in, Collector<Tuple6<Integer, String, Float, Integer, Integer, Date>> out) throws Exception {
        DateFormat format = new SimpleDateFormat("yyyy-mm-dd");
        out.collect(new Tuple6(in.f0, in.f1, in.f2, in.f3, in.f4, format.parse(in.f5)));
    }
}