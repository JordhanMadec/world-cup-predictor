package insa.rennes;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.api.java.tuple.Tuple9;
import org.apache.flink.util.Collector;

import java.time.LocalDate;
import java.time.ZoneId;
import java.util.Date;

public class InternationalResultsStats implements FlatMapFunction<
    Tuple9<Date, String, String, Integer, Integer, String, String, String, Boolean>,
    Tuple7<String, Integer, Integer, Integer, Integer, Integer, Integer>
> {

    @Override
    public void flatMap(
            Tuple9<Date, String, String, Integer, Integer, String, String, String, Boolean> in,
            Collector<Tuple7<String, Integer, Integer, Integer, Integer, Integer, Integer>> out
    ) throws Exception {

        // Old version
        // (country, win, draw, loss, goals_for, goals_against, wc_win, wc_draw, wc_loss, wc_goals_for, wc_goals_against)

        // New version
        // (country, world cup edition, win, draw, loss, goals_for, goals_against)
        // world cup edition = 4-year timelapse

        // Home team

        int year = in.f0.toInstant().atZone(ZoneId.systemDefault()).toLocalDate().getYear();
        int edition = Utils.getWorldCupEdition(year);

        if (edition > 0) {

            out.collect(new Tuple7(
                    in.f1, // country
                    edition,
                    (in.f3 > in.f4 ? 1 : 0), // win
                    (in.f3 == in.f4 ? 1 : 0), // draw
                    (in.f3 < in.f4 ? 1 : 0), // loss
                    in.f3, // goals_for
                    in.f4 //goals_against
            ));

            // Away Team
            out.collect(new Tuple7(
                    in.f2, // country
                    edition,
                    (in.f4 > in.f3 ? 1 : 0), // win
                    (in.f4 == in.f3 ? 1 : 0), // draw
                    (in.f4 < in.f3 ? 1 : 0), // loss
                    in.f4, // goals_for
                    in.f3 //goals_against
            ));
        }
    }
}