/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package insa.rennes;

import insa.rennes.cosine.similarity.*;
import insa.rennes.vectors.*;
import insa.rennes.fifa.ranking.FifaRankingDateConverter;
import insa.rennes.fifa.ranking.FifaRankingStats;
import insa.rennes.fifa.ranking.FifaRankingStatsReduce;
import insa.rennes.international.results.InternationalResultsDateConverter;
import insa.rennes.international.results.InternationalResultsStats;
import insa.rennes.international.results.InternationalResultsStatsReduce;
import insa.rennes.winners.*;
import insa.rennes.world.cup.history.WorldCupHistoryStatsReduce;
import insa.rennes.world.cup.history.WorldcupHistoryEliminatesDouble;
import insa.rennes.world.cup.history.WorldcupHistoryStats;
import insa.rennes.world.cup.history.WorldcupWinners;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.*;

public class BatchJob {

	public static void main(String[] args) throws Exception {
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();



		// ----- PARSING -----

		// (team, edition, rank average, rank evolution)
		DataSet<Tuple4<String, Integer, Double, Integer>> fifaRanks;
		// (team, edition, win ratio, loss ratio, goals ratio)
		DataSet<Tuple5<String, Integer, Double, Double, Double>> internationalResults;
		// (team, edition, finals ratio, semi finals ratio)
		DataSet<Tuple4<String, Integer, Double, Double>> worldcupHistory;



		// ----- ALL VECTORS -----

		// (team, edition, rank weight, win ratio, loss ratio, goals ratio, finals ratio, semi finals ratio)
		DataSet<Tuple8<String, Integer, Double, Double, Double, Double, Double, Double>> allVectors;
		// (team, edition, win ratio, loss ratio, goals ratio, finals ratio, semi finals ratio)
		DataSet<Tuple7<String, Integer, Double, Double, Double, Double, Double>> allVectorsNoRanking;



		// ----- WINNERS VECTORS -----

		// (team, edition)
		DataSet<Tuple2<String, Integer>> winners;

		// (rank weight, win ratio, loss ratio, goals ratio, finals ratio, semi finals ratio)
		DataSet<Tuple6<Double, Double, Double, Double, Double, Double>> winnerVector;
		// (win ratio, loss ratio, goals ratio, finals ratio, semi finals ratio)
		DataSet<Tuple5<Double, Double, Double, Double, Double>> winnerVectorNoRanking;



		// ----- COSINE SIMILARITY -----

		// (team, edition, cosine similarity)
		DataSet<Tuple3<String, Integer, Double>> cosineSimilarity;
		// (team, edition, cosine similarity)
		DataSet<Tuple3<String, Integer, Double>> cosineSimilarityNoRanking;






		fifaRanks = env.readCsvFile(Settings.fifaRanksPath)
				.ignoreFirstLine()
				.types(Integer.class, String.class, Float.class, Integer.class, Integer.class, String.class)
				.flatMap(new FifaRankingDateConverter())
				.flatMap(new FifaRankingStats())
				.groupBy(0, 1)
				.reduceGroup(new FifaRankingStatsReduce());

		internationalResults = env.readCsvFile(Settings.internationalResultsPath)
				.ignoreFirstLine()
				.types(String.class, String.class, String.class, Integer.class, Integer.class, String.class, String.class, String.class, Boolean.class)
				.flatMap(new InternationalResultsDateConverter())
				.flatMap(new InternationalResultsStats())
				.groupBy(0, 1)
				.reduceGroup(new InternationalResultsStatsReduce());

		worldcupHistory = env.readCsvFile(Settings.worldcupHistoryPath)
				.ignoreFirstLine()
				.types(Integer.class, String.class, String.class, String.class, String.class, String.class, Integer.class, Integer.class, Integer.class, Float.class)
				.flatMap(new WorldcupHistoryStats())
				.groupBy(0)
				.sortGroup(1, Order.ASCENDING)
				.reduceGroup(new WorldCupHistoryStatsReduce())
				.distinct()
				.groupBy(0,1)
				.reduceGroup(new WorldcupHistoryEliminatesDouble());



		allVectors = fifaRanks.join(internationalResults)
				.where(0, 1)
				.equalTo(0, 1)
				.with(new JoinRanksAndResults())
				.join(worldcupHistory)
				.where(0,1)
				.equalTo(0,1)
				.with(new Vectors());

		allVectorsNoRanking = allVectors
				.map(new VectorsNoRanking());



		winners = env.readCsvFile(Settings.worldcupHistoryPath)
				.ignoreFirstLine()
				.types(Integer.class, String.class, String.class, String.class, String.class, String.class, Integer.class, Integer.class, Integer.class, Float.class)
				.flatMap(new WorldcupWinners());

		winnerVector = allVectors
				.join(winners)
				.where(0,1)
				.equalTo(0,1)
				.with(new JoinWinners())
				.map(new Normalize())
				.reduceGroup(new WinnerReduce());

		winnerVectorNoRanking = allVectors
				.join(winners)
				.where(0,1)
				.equalTo(0,1)
				.with(new JoinWinners())
				.map(new VectorsNoRanking())
				.map(new NormalizeNoRanking())
				.reduceGroup(new WinnerNoRankingReduce());



		cosineSimilarity = allVectors
				//.filter(new FilterWorldcupEdition())
				.map(new Normalize())
				.map(new CosineSimilarity())
				.sortPartition(2, Order.DESCENDING)
				.setParallelism(1);

		cosineSimilarityNoRanking = allVectors
				.filter(new FilterWorldcupEdition())
				.map(new VectorsNoRanking())
				.map(new NormalizeNoRanking())
				.map(new CosineSimilarityNoRanking())
				.sortPartition(2, Order.DESCENDING)
				.setParallelism(1);





		//internationalResults.print();
		//fifaRanks.print();
		//worldcupHistory.print();

		//allVectors.print();
		//allVectorsNoRanking.print();

		//winners.print();
		//winnerVector.print();
		//winnerVectorNoRanking.print();

		//cosineSimilarity.first(20).print();
		//cosineSimilarityNoRanking.first(20).print();

		cosineSimilarity.writeAsCsv("file:///Users/jordhanmadec/dev/INSA/world-cup-predictor/worldcup_predictions.csv", "\n", ",");
		env.execute("Worldcup Predicts");
	}





	// Winner vector with ranking (since 1994)
	// (0.021958816901128827,0.5463012214106105,0.1271657951548444,0.6253181209341707,0.3618595676038736,0.21318429150103246)

	// Winner vector without ranking (since 1930)
	// (0.5426847822708153,0.12378319465552902,0.61986720916081,0.41307955459901263)

}
