package insa.rennes.vectors;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.tuple.Tuple7;

public class JoinRanksAndResults implements JoinFunction<
        Tuple4<String, Integer, Double, Integer>,
        Tuple5<String, Integer, Double, Double, Double>,
        Tuple7<String, Integer, Double, Integer, Double, Double, Double>> {

    @Override
    public Tuple7<String, Integer, Double, Integer, Double, Double, Double> join(
            Tuple4<String, Integer, Double, Integer> in1,
            Tuple5<String, Integer, Double, Double, Double> in2
    ) throws Exception {
        return new Tuple7(in1.f0, in1.f1, in1.f2, in1.f3, in2.f2, in2.f3, in2.f4);
    }
}
