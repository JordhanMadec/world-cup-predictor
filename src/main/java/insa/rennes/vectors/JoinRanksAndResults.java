package insa.rennes.vectors;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.api.java.tuple.Tuple7;

public class JoinRanksAndResults implements JoinFunction<
        Tuple4<String, Integer, Double, Integer>,
        Tuple6<String, Integer, Double, Double, Double, Double>,
        Tuple7<String, Integer, Double, Double, Double, Double, Double>> {

    @Override
    public Tuple7<String, Integer, Double, Double, Double, Double, Double> join(
            Tuple4<String, Integer, Double, Integer> in1,
            Tuple6<String, Integer, Double, Double, Double, Double> in2
    ) throws Exception {
        double rank = in1.f2;
        double rankEvolution = in1.f3 * 1.0;

        rankEvolution = rankEvolution == 0.0 ? 1.0 : rankEvolution;
        double rankWeight = rankEvolution / Math.exp(rank);

        return new Tuple7(in1.f0, in1.f1, rankWeight, in2.f2, in2.f3, in2.f4, in2.f5);
    }
}
