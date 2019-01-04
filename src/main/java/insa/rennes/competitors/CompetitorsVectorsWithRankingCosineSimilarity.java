package insa.rennes.competitors;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple10;
import org.apache.flink.api.java.tuple.Tuple3;

public class CompetitorsVectorsWithRankingCosineSimilarity implements MapFunction<
        Tuple10<String, Integer, Double, Double, Double, Double, Double, Double, Double, Double>,
        Tuple3<String, Integer, Double>> {
    @Override
    public Tuple3<String, Integer, Double> map(
            Tuple10<String, Integer, Double, Double, Double, Double, Double, Double, Double, Double> in
    ) throws Exception {
        final double[] winnerVector = { 0.49893470671731455,0.7290825086499529,0.07050037474835713,0.012528329017067418,0.07742714687235631,0.39121500464143816,0.23117250274266798,0.04348244694445422 };

        double sumCompetitor = in.f2 * in.f2 + in.f3 * in.f3 + in.f4 * in.f4 + in.f5 * in.f5 + in.f6 * in.f6 + in.f7 * in.f7 + in.f8 * in.f8 + in.f9 * in.f9;

        double sumWinner = 0.0;
        for(double i : winnerVector) sumWinner += i*i;

        double similarity = in.f2 * winnerVector[0]
                + in.f3 * winnerVector[1]
                + in.f4 * winnerVector[2]
                + in.f5 * winnerVector[3]
                + in.f6 * winnerVector[4]
                + in.f7 * winnerVector[5]
                + in.f8 * winnerVector[6]
                + in.f9 * winnerVector[7];

        similarity = similarity / (Math.sqrt(sumCompetitor) * Math.sqrt(sumWinner));

        return new Tuple3(in.f0, in.f1, similarity);
    }
}
