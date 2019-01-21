package insa.rennes.vectors;

import insa.rennes.Settings;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.api.java.tuple.Tuple10;

public class Vectors implements JoinFunction<
        Tuple7<String, Integer, Double, Double, Double, Double, Double>,
        Tuple5<String, Integer, Double, Double, Double>,
        Tuple10<String, Integer, Double, Double, Double, Double, Double, Double, Double, Double>> {

    @Override
    public Tuple10<String, Integer, Double, Double, Double, Double, Double, Double, Double, Double> join(
            Tuple7<String, Integer, Double, Double, Double, Double, Double> in1,
            Tuple5<String, Integer, Double, Double, Double> in2
    ) throws Exception {
        // (rank weight, win ratio, loss ratio, goals ratio, goals per match, finals ratio, semi finals ratio, hosting)
        return new Tuple10(in1.f0, in1.f1,
                in1.f2 * Settings.RANK_WEIGHT,
                in1.f3 * Settings.WIN_WEIGHT,
                in1.f4 * Settings.LOSS_WEIGHT,
                in1.f5 * Settings.GOALS_WEIGHT,
                in1.f6 * Settings.GOALS_PER_MATCH_WEIGHT,
                in2.f2 * Settings.FINAL_WEIGHT,
                in2.f3 * Settings.SEMI_FINAL_WEIGHT,
                in2.f4 * Settings.HOSTING_WEIGHT);
    }
}
