package insa.rennes.cosine.similarity;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.api.java.tuple.Tuple3;

public class CosineSimilarity implements MapFunction<
        Tuple8<String, Integer, Double, Double, Double, Double, Double, Double>,
        Tuple3<String, Integer, Double>> {
    @Override
    public Tuple3<String, Integer, Double> map(
            Tuple8<String, Integer, Double, Double, Double, Double, Double, Double> in
    ) throws Exception {
        final double[] winnerVector = { 0.02250297340695942,0.6049770896782736,0.14737301783083326,0.6966904017283677,0.21633813015320522,0.12309245111950072 };

        double sumCompetitor = in.f2 * in.f2 + in.f3 * in.f3 + in.f4 * in.f4 + in.f5 * in.f5 + in.f6 * in.f6 + in.f7 * in.f7;

        double sumWinner = 0.0;
        for(double i : winnerVector) sumWinner += i*i;

        double similarity = in.f2 * winnerVector[0]
                + in.f3 * winnerVector[1]
                + in.f4 * winnerVector[2]
                + in.f5 * winnerVector[3]
                + in.f6 * winnerVector[4]
                + in.f7 * winnerVector[5];

        similarity = similarity / (Math.sqrt(sumCompetitor) * Math.sqrt(sumWinner));

        return new Tuple3(in.f0, in.f1, similarity);
    }
}
