package insa.rennes;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple9;
import org.apache.flink.util.Collector;

import java.util.Date;

public class InternationalResultsStats implements FlatMapFunction<Tuple9<Date, String, String, Integer, Integer, String, String, String, Boolean>, Tuple3<String, String, Integer>> {
    @Override
    public void flatMap(Tuple9<Date, String, String, Integer, Integer, String, String, String, Boolean> in, Collector<Tuple3<String, String, Integer>> out) throws Exception {
        if (in.f3 > in.f4) {
            out.collect(new Tuple3(in.f1, "win", 1));
            out.collect(new Tuple3(in.f2, "loss", 1));
            if (in.f5.equals("FIFA World Cup")) {
                out.collect(new Tuple3(in.f1, "wc_win", 1));
                out.collect(new Tuple3(in.f2, "wc_loss", 1));
            }
        } else if (in.f3 < in.f4) {
            out.collect(new Tuple3(in.f2, "win", 1));
            out.collect(new Tuple3(in.f1, "loss", 1));
            if (in.f5.equals("FIFA World Cup")) {
                out.collect(new Tuple3(in.f2, "wc_win", 1));
                out.collect(new Tuple3(in.f1, "wc_loss", 1));
            }
        } else {
            out.collect(new Tuple3(in.f1, "draw", 1));
            out.collect(new Tuple3(in.f2, "draw", 1));
            if (in.f5.equals("FIFA World Cup")) {
                out.collect(new Tuple3(in.f1, "wc_draw", 1));
                out.collect(new Tuple3(in.f2, "wc_draw", 1));
            }
        }

        out.collect(new Tuple3(in.f1, "goals_for", in.f3));
        out.collect(new Tuple3(in.f2, "goals_for", in.f4));
        out.collect(new Tuple3(in.f1, "goals_against", in.f4));
        out.collect(new Tuple3(in.f2, "goals_against", in.f3));

        if (in.f5.equals("FIFA World Cup")) {
            out.collect(new Tuple3(in.f1, "wc_goals_for", in.f3));
            out.collect(new Tuple3(in.f2, "wc_goals_for", in.f4));
            out.collect(new Tuple3(in.f1, "wc_goals_against", in.f4));
            out.collect(new Tuple3(in.f2, "wc_goals_against", in.f3));
        }
    }
}