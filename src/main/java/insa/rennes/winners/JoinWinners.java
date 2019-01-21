package insa.rennes.winners;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple10;
import org.apache.flink.api.java.tuple.Tuple2;

public class JoinWinners implements JoinFunction<
        Tuple10<String, Integer, Double, Double, Double, Double, Double, Double, Double, Double>,
        Tuple2<String, Integer>,
        Tuple10<String, Integer, Double, Double, Double, Double, Double, Double, Double, Double>> {

    @Override
    public Tuple10<String, Integer, Double, Double, Double, Double, Double, Double, Double, Double> join(
            Tuple10<String, Integer, Double, Double, Double, Double, Double, Double, Double, Double> in1,
            Tuple2<String, Integer> in2
    ) throws Exception {
        return in1;
    }
}
