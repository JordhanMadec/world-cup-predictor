package insa.rennes;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;

public class FifaRankingAverage implements GroupReduceFunction<Tuple3<String, Integer, Integer>, Tuple2<String, Integer>> {

    @Override
    public void reduce(Iterable<Tuple3<String, Integer, Integer>> in, Collector<Tuple2<String, Integer>> out) {
        int rankSum = 0;
        int count = 0;
        String country = "";

        for(Tuple3<String, Integer, Integer> tuple: in) {
            country = tuple.f0;
            rankSum += tuple.f1;
            count += tuple.f2;
        }

        out.collect(new Tuple2(country, rankSum/count));
    }
}
