package insa.rennes.competitors;

import insa.rennes.Settings;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.tuple.Tuple5;

public class FilterWorldcupEdition implements FilterFunction<Tuple5<String, Integer, Double, Double, Double>> {
    @Override
    public boolean filter(Tuple5<String, Integer, Double, Double, Double> in) throws Exception {
        return in.f1 == Settings.EDITION;
    }
}
