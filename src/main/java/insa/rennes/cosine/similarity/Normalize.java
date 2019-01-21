package insa.rennes.cosine.similarity;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple10;

public class Normalize implements MapFunction<
        Tuple10<String, Integer, Double, Double, Double, Double, Double, Double, Double, Double>,
        Tuple10<String, Integer, Double, Double, Double, Double, Double, Double, Double, Double>> {
    @Override
    public Tuple10<String, Integer, Double, Double, Double, Double, Double, Double, Double, Double> map(
            Tuple10<String, Integer, Double, Double, Double, Double, Double, Double, Double, Double> in
    ) throws Exception {


        double norm = Math.sqrt(in.f2*in.f2 + in.f3*in.f3 + in.f4*in.f4 + in.f5*in.f5 + in.f6*in.f6 + in.f7 * in.f7 + in.f8 * in.f8);

        // Exclude hosting

        // (team, edition, rank weight, win ratio, loss ratio, goals ratio, finals ratio, semi finals ratio, hosting)
        return new Tuple10(in.f0, in.f1, in.f2/norm, in.f3/norm, in.f4/norm, in.f5/norm, in.f6/norm, in.f7/norm, in.f8/norm, 0.0);
    }
}
