package insa.rennes.vectors;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.api.java.tuple.Tuple5;

public class FinalistsVectorsWithoutRanking implements JoinFunction<
        Tuple5<String, Integer, Double, Double, Double>,
        Tuple5<String, Integer, Integer, Integer, Double>,
        Tuple8<String, Integer, Double, Double, Double, Integer, Integer, Double>> {

    @Override
    public Tuple8<String, Integer, Double, Double, Double, Integer, Integer, Double> join(
            Tuple5<String, Integer, Double, Double, Double> in1,
            Tuple5<String, Integer, Integer, Integer, Double> in2
    ) throws Exception {
        return new Tuple8(in1.f0, in1.f1, in1.f2, in1.f3, in1.f4, in2.f2, in2.f3, in2.f4);
    }
}
