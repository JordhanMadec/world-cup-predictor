package insa.rennes.competitors;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple10;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.tuple.Tuple7;

public class CompetitorsVectorsWithRanking implements JoinFunction<
        Tuple7<String, Integer, Double, Integer, Double, Double, Double>,
        Tuple5<String, Integer, Integer, Integer, Double>,
        Tuple10<String, Integer, Double, Integer, Double, Double, Double, Integer, Integer, Double>> {

    @Override
    public Tuple10<String, Integer, Double, Integer, Double, Double, Double, Integer, Integer, Double> join(
            Tuple7<String, Integer, Double, Integer, Double, Double, Double> in1,
            Tuple5<String, Integer, Integer, Integer, Double> in2
    ) throws Exception {
        return new Tuple10(in1.f0, in1.f1, in1.f2, in1.f3, in1.f4, in1.f5, in1.f6, in2.f2, in2.f3, in2.f4);
    }
}
