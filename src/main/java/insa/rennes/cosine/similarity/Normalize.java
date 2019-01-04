package insa.rennes.cosine.similarity;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple8;

public class Normalize implements MapFunction<
        Tuple8<String, Integer, Double, Integer, Double, Double, Double, Double>,
        Tuple8<String, Integer, Double, Double, Double, Double, Double, Double>> {
    @Override
    public Tuple8<String, Integer, Double, Double, Double, Double, Double, Double> map(
            Tuple8<String, Integer, Double, Integer, Double, Double, Double, Double> in
    ) throws Exception {
        double norm = Math.sqrt(in.f2*in.f2 + in.f3*in.f3 + in.f4*in.f4 + in.f5*in.f5 + in.f6*in.f6 + in.f7*in.f7);

        return new Tuple8(in.f0, in.f1, in.f2/norm, in.f3/norm, in.f4/norm, in.f5/norm, in.f6/norm, in.f7/norm);
    }
}
