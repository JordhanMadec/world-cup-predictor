package insa.rennes.winners;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple8;

public class WinnersVectorsWithoutRanking implements JoinFunction<
        Tuple8<String, Integer, Double, Double, Double, Integer, Integer, Double>,
        Tuple2<String, Integer>,
        Tuple8<String, Integer, Double, Double, Double, Integer, Integer, Double>> {

    @Override
    public Tuple8<String, Integer, Double, Double, Double, Integer, Integer, Double> join(
            Tuple8<String, Integer, Double, Double, Double, Integer, Integer, Double> in1,
            Tuple2<String, Integer> in2
    ) throws Exception {
        return in1;
    }
}
