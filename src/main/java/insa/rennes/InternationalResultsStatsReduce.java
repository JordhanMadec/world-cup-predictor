package insa.rennes;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple11;
import org.apache.flink.util.Collector;

public class InternationalResultsStatsReduce implements GroupReduceFunction<
    Tuple11<String, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>,
    Tuple11<String, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>
> {

    @Override
    public void reduce(
        Iterable<Tuple11<String, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>> iterable,
        Collector<Tuple11<String, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer>> out
    ) throws Exception {
        Tuple11<String, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer> result = new Tuple11("", 0, 0, 0, 0, 0, 0, 0, 0, 0, 0);

        for(Tuple11<String, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer, Integer> tuple: iterable) {
            result = new Tuple11(
                    tuple.f0,
                    result.f1 + tuple.f1,
                    result.f2 + tuple.f2,
                    result.f3 + tuple.f3,
                    result.f4 + tuple.f4,
                    result.f5 + tuple.f5,
                    result.f6 + tuple.f6,
                    result.f7 + tuple.f7,
                    result.f8 + tuple.f8,
                    result.f9 + tuple.f9,
                    result.f10 + tuple.f10
            );
        }

        out.collect(result);
    }
}
