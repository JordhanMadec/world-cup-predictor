package insa.rennes.cosine.similarity;

import insa.rennes.Settings;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.tuple.Tuple10;

public class FilterWorldcupEdition implements FilterFunction<Tuple10<String, Integer, Double, Integer, Double, Double, Double, Integer, Integer, Double>> {
    @Override
    public boolean filter(Tuple10<String, Integer, Double, Integer, Double, Double, Double, Integer, Integer, Double> in) throws Exception {
        return in.f1 == Settings.EDITION;
    }
}
