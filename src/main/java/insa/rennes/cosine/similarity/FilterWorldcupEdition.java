package insa.rennes.cosine.similarity;

import insa.rennes.Settings;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.tuple.Tuple8;

public class FilterWorldcupEdition implements FilterFunction<Tuple8<String, Integer, Double, Integer, Double, Double, Double, Double>> {
    @Override
    public boolean filter(Tuple8<String, Integer, Double, Integer, Double, Double, Double, Double> in) throws Exception {
        return in.f1 == Settings.EDITION;
    }
}
