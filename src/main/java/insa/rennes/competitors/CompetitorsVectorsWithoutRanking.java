package insa.rennes.competitors;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple10;
import org.apache.flink.api.java.tuple.Tuple8;

public class CompetitorsVectorsWithoutRanking implements MapFunction<
        Tuple10<String, Integer,Double, Integer, Double, Double, Double, Integer, Integer, Double>,
        Tuple8<String, Integer, Double, Double, Double, Integer, Integer, Double>> {
    @Override
    public Tuple8<String, Integer, Double, Double, Double, Integer, Integer, Double> map(
            Tuple10<String, Integer, Double, Integer, Double, Double, Double, Integer, Integer, Double> in
    ) throws Exception {
        return new Tuple8(in.f0, in.f1, in.f4, in.f5, in.f6, in.f7, in.f8, in.f9);
    }
}

