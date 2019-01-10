package insa.rennes.vectors;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.tuple.Tuple6;

public class JoinRanksAndResults implements JoinFunction<
        Tuple4<String, Integer, Double, Integer>,
        Tuple5<String, Integer, Double, Double, Double>,
        Tuple6<String, Integer, Double, Double, Double, Double>> {

    @Override
    public Tuple6<String, Integer, Double, Double, Double, Double> join(
            Tuple4<String, Integer, Double, Integer> in1,
            Tuple5<String, Integer, Double, Double, Double> in2
    ) throws Exception {
        double rank = in1.f2;
        double rankEvolution = in1.f3 * 1.0;

        rankEvolution = rankEvolution == 0.0 ? 1.0 : rankEvolution;
        double rankWeight = rankEvolution / Math.exp(rank);

        return new Tuple6(in1.f0, in1.f1, rankWeight, in2.f2, in2.f3, in2.f4);
    }
}
