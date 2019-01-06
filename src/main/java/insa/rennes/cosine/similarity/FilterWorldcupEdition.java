package insa.rennes.cosine.similarity;

import insa.rennes.Settings;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.tuple.Tuple7;

public class FilterWorldcupEdition implements FilterFunction<Tuple7<String, Integer, Double, Double, Double, Double, Double>> {
    @Override
    public boolean filter(Tuple7<String, Integer, Double, Double, Double, Double, Double> in) throws Exception {
        return in.f1 == Settings.EDITION;
    }
}
