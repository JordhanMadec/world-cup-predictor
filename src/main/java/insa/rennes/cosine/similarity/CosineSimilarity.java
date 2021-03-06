package insa.rennes.cosine.similarity;

import org.apache.flink.api.common.functions.CrossFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.api.java.tuple.Tuple10;

public class CosineSimilarity implements CrossFunction<Tuple10<String, Integer, Double, Double, Double, Double, Double, Double, Double, Double>, Tuple8<Double, Double, Double, Double, Double, Double, Double, Double>, Tuple3<String, Integer, Double>> {
    @Override
    public Tuple3<String, Integer, Double> cross(Tuple10<String, Integer, Double, Double, Double, Double, Double, Double, Double, Double> challenger, Tuple8<Double, Double, Double, Double, Double, Double, Double, Double> winner) throws Exception {
        double sumChalleneger = challenger.f2 * challenger.f2 + challenger.f3 * challenger.f3 + challenger.f4 * challenger.f4 + challenger.f5 * challenger.f5 + challenger.f6 * challenger.f6 + challenger.f7 * challenger.f7 + challenger.f8 * challenger.f8 + challenger.f9 * challenger.f9;
        double sumWinner = winner.f0 * winner.f0 + winner.f1 * winner.f1 + winner.f2 * winner.f2 + winner.f3 * winner.f3 + winner.f4 * winner.f4 + winner.f5 * winner.f5 + winner.f6 * winner.f6 + winner.f7 * winner.f7;
        
        double similarity = challenger.f2 * winner.f0
                + challenger.f3 * winner.f1
                + challenger.f4 * winner.f2
                + challenger.f5 * winner.f3
                + challenger.f6 * winner.f4
                + challenger.f7 * winner.f5
                + challenger.f8 * winner.f6
                + challenger.f9 * winner.f7;

        similarity = similarity / (Math.sqrt(sumChalleneger) * Math.sqrt(sumWinner));

        return new Tuple3(challenger.f0, challenger.f1, similarity);
    }
}
