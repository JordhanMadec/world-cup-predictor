package insa.rennes.vectors;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple10;
import org.apache.flink.api.java.tuple.Tuple2;

public class WinnersVectorsWithoutRanking implements JoinFunction<
        Tuple10<String, Integer, Double, Integer, Double, Double, Double, Integer, Integer, Double>,
        Tuple2<String, Integer>,
        Tuple10<String, Integer, Double, Integer, Double, Double, Double, Integer, Integer, Double>> {

    @Override
    public Tuple10<String, Integer, Double, Integer, Double, Double, Double, Integer, Integer, Double> join(
            Tuple10<String, Integer, Double, Integer, Double, Double, Double, Integer, Integer, Double> in1,
            Tuple2<String, Integer> in2
    ) throws Exception {
        return in1;
    }
}
