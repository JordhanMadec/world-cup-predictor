package insa.rennes;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple11;
import org.apache.flink.api.java.tuple.Tuple9;
import org.apache.flink.util.Collector;

import java.util.Date;

public class InternationalResultsStats implements FlatMapFunction<
    Tuple9<Date, String, String, Integer, Integer, String, String, String, Boolean>,
    Tuple11<String, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>
> {

    @Override
    public void flatMap(
            Tuple9<Date, String, String, Integer, Integer, String, String, String, Boolean> in,
            Collector<Tuple11<String, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>> out
    ) throws Exception {

        // (country, win, draw, loss, goals_for, goals_against, wc_win, wc_draw, wc_loss, wc_goals_for, wc_goals_against)

        if (in.f5.equals("FIFA World Cup")) {
            // Home team
            out.collect(new Tuple11(
                    in.f1, // country
                    (in.f3 > in.f4 ? 1 : 0), // win
                    (in.f3 == in.f4 ? 1 : 0), // draw
                    (in.f3 < in.f4 ? 1 : 0), // loss
                    in.f3, // goals_for
                    in.f4, //goals_against
                    (in.f3 > in.f4 ? 1 : 0), // wc_win
                    (in.f3 == in.f4 ? 1 : 0), // wc_draw
                    (in.f3 < in.f4 ? 1 : 0), // wc_loss
                    in.f3, // wc_goals_for
                    in.f4 // wc_goals_against
            ));

            // Away Team
            out.collect(new Tuple11(
                    in.f2, // country
                    (in.f4 > in.f3 ? 1 : 0), // win
                    (in.f4 == in.f3 ? 1 : 0), // draw
                    (in.f4 < in.f3 ? 1 : 0), // loss
                    in.f4, // goals_for
                    in.f3, //goals_against
                    (in.f4 > in.f3 ? 1 : 0), // wc_win
                    (in.f4 == in.f3 ? 1 : 0), // wc_draw
                    (in.f4 < in.f3 ? 1 : 0), // wc_loss
                    in.f4, // wc_goals_for
                    in.f3 // wc_goals_against
            ));
        } else {
            // Home team
            out.collect(new Tuple11(
                    in.f1, // country
                    (in.f3 > in.f4 ? 1 : 0), // win
                    (in.f3 == in.f4 ? 1 : 0), // draw
                    (in.f3 < in.f4 ? 1 : 0), // loss
                    in.f3, // goals_for
                    in.f4, //goals_against
                    0, // wc_win
                    0, // wc_draw
                    0, // wc_loss
                    0, // wc_goals_for
                    0 // wc_goals_against
            ));

            // Away Team
            out.collect(new Tuple11(
                    in.f2, // country
                    (in.f4 > in.f3 ? 1 : 0), // win
                    (in.f4 == in.f3 ? 1 : 0), // draw
                    (in.f4 < in.f3 ? 1 : 0), // loss
                    in.f4, // goals_for
                    in.f3, //goals_against
                    0, // wc_win
                    0, // wc_draw
                    0, // wc_loss
                    0, // wc_goals_for
                    0 // wc_goals_against
            ));
        }
    }
}