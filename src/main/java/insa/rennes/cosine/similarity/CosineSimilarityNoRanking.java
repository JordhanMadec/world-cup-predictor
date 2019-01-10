package insa.rennes.cosine.similarity;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple7;

public class CosineSimilarityNoRanking implements MapFunction<
        Tuple7<String, Integer, Double, Double, Double, Double, Double>,
        Tuple3<String, Integer, Double>> {
    @Override
    public Tuple3<String, Integer, Double> map(
            Tuple7<String, Integer, Double, Double, Double, Double, Double> in
    ) throws Exception {
        final double[] winnerVector = { 0.5426847822708153,0.12378319465552902,0.61986720916081,0.41307955459901263 };

        double sumCompetitor = in.f2 * in.f2 + in.f3 * in.f3 + in.f4 * in.f4 + in.f5 * in.f5 + in.f6 * in.f6;

        double sumWinner = 0.0;
        for(double i : winnerVector) sumWinner += i*i;

        double similarity = in.f2 * winnerVector[0]
                + in.f3 * winnerVector[1]
                + in.f4 * winnerVector[2]
                + in.f5 * winnerVector[3]
                + in.f6 * winnerVector[4];

        similarity = similarity / (Math.sqrt(sumCompetitor) * Math.sqrt(sumWinner));

        return new Tuple3(in.f0, in.f1, similarity);
    }
}
