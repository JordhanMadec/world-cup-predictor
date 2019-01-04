package insa.rennes.vectors;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.tuple.Tuple7;

public class Vectors implements JoinFunction<
        Tuple7<String, Integer, Double, Integer, Double, Double, Double>,
        Tuple5<String, Integer, Integer, Integer, Double>,
        Tuple8<String, Integer, Double, Integer, Double, Double, Double, Double>> {

    @Override
    public Tuple8<String, Integer, Double, Integer, Double, Double, Double, Double> join(
            Tuple7<String, Integer, Double, Integer, Double, Double, Double> in1,
            Tuple5<String, Integer, Integer, Integer, Double> in2
    ) throws Exception {
        // (rank average, rank evolution, win ratio, loss ratio, goals ratio, finals ratio)
        return new Tuple8(in1.f0, in1.f1, in1.f2, in1.f3, in1.f4, in1.f5, in1.f6, in2.f4);
    }
}
