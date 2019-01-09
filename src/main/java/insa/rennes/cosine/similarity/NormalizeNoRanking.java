package insa.rennes.cosine.similarity;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple7;

public class NormalizeNoRanking implements MapFunction<
        Tuple7<String, Integer, Double, Double, Double, Double, Double>,
        Tuple7<String, Integer, Double, Double, Double, Double, Double>> {
    @Override
    public Tuple7<String, Integer, Double, Double, Double, Double, Double> map(
            Tuple7<String, Integer, Double, Double, Double, Double, Double> in
    ) throws Exception {
        double norm = Math.sqrt(in.f2*in.f2 + in.f3*in.f3 + in.f4*in.f4 + in.f5*in.f5 + in.f6*in.f6);

        return new Tuple7(in.f0, in.f1, in.f2/norm, in.f3/norm, in.f4/norm, in.f5/norm, in.f6/norm);
    }
}
