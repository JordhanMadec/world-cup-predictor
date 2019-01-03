package insa.rennes.internationalResults;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple9;
import org.apache.flink.util.Collector;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

public class InternationalResultsDateConverter implements FlatMapFunction<Tuple9<String, String, String, Integer, Integer, String, String, String, Boolean>, Tuple9<Date, String, String, Integer, Integer, String, String, String, Boolean>> {
    @Override
    public void flatMap(Tuple9<String, String, String, Integer, Integer, String, String, String, Boolean> in, Collector<Tuple9<Date, String, String, Integer, Integer, String, String, String, Boolean>> out) throws Exception {
        DateFormat format = new SimpleDateFormat("yyyy-mm-dd");
        out.collect(new Tuple9(format.parse(in.f0), in.f1, in.f2, in.f3, in.f4, in.f5, in.f6, in.f7, in.f8));
    }
}