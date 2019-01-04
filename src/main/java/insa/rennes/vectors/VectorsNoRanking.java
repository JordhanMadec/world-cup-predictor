package insa.rennes.vectors;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.api.java.tuple.Tuple6;

public class VectorsNoRanking implements MapFunction<
        Tuple8<String, Integer,Double, Integer, Double, Double, Double, Double>,
        Tuple6<String, Integer, Double, Double, Double, Double>> {
    @Override
    public Tuple6<String, Integer, Double, Double, Double, Double> map(
            Tuple8<String, Integer, Double, Integer, Double, Double, Double, Double> in
    ) throws Exception {
        return new Tuple6(in.f0, in.f1, in.f4, in.f5, in.f6, in.f7);
    }
}

