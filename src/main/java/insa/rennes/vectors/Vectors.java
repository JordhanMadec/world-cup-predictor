package insa.rennes.vectors;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.api.java.tuple.Tuple8;

public class Vectors implements JoinFunction<
        Tuple6<String, Integer, Double, Double, Double, Double>,
        Tuple4<String, Integer, Double, Double>,
        Tuple8<String, Integer, Double, Double, Double, Double, Double, Double>> {

    @Override
    public Tuple8<String, Integer, Double, Double, Double, Double, Double, Double> join(
            Tuple6<String, Integer, Double, Double, Double, Double> in1,
            Tuple4<String, Integer, Double, Double> in2
    ) throws Exception {
        // (rank weight, win ratio, loss ratio, goals ratio, finals ratio, semi finals ratio)

        return new Tuple8(in1.f0, in1.f1, in1.f2, in1.f3, in1.f4, in1.f5, in2.f2, in2.f3);
    }
}
