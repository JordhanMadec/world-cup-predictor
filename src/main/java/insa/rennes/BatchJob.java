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

import insa.rennes.competitors.FilterWorldcupEdition;
import insa.rennes.fifaRanking.FifaRankingDateConverter;
import insa.rennes.fifaRanking.FifaRankingStats;
import insa.rennes.fifaRanking.FifaRankingStatsReduce;
import insa.rennes.internationalResults.InternationalResultsDateConverter;
import insa.rennes.internationalResults.InternationalResultsStats;
import insa.rennes.internationalResults.InternationalResultsStatsReduce;
import insa.rennes.winners.*;
import insa.rennes.worldCupHistory.WorldCupHistoryStatsReduce;
import insa.rennes.worldCupHistory.WorldcupHistoryStats;
import insa.rennes.worldCupHistory.WorldcupWinners;
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
		// (team, edition, finals played, finals won, ratio)
		DataSet<Tuple5<String, Integer, Integer, Integer, Double>> worldcupHistory;



		// ----- WINNERS VECTORS -----

		// (team, edition)
		DataSet<Tuple2<String, Integer>> winners;

		// (team, edition, rank average, rank evolution, win ratio, loss ratio, goals ratio, finals played, finals won, ratio)
		DataSet<Tuple10<String, Integer, Double, Integer, Double, Double, Double, Integer, Integer, Double>> winnersVectorsWithRanking;
		// (team, edition, win ratio, loss ratio, goals ratio, finals played, finals won, ratio)
		DataSet<Tuple8<String, Integer, Double, Double, Double, Integer, Integer, Double>> winnersVectorsWithoutRanking;

		// (rank average, rank evolution, win ratio, loss ratio, goals ratio, finals played, finals won, ratio)
		DataSet<Tuple8<Double, Double, Double, Double, Double, Double, Double, Double>> winnerVectorWithRanking;
		// (win ratio, loss ratio, goals ratio, finals played, finals won, ratio)
		DataSet<Tuple6<Double, Double, Double, Double, Double, Double>> winnerVectorWithoutRanking;



		// ----- COMPETITORS VECTORS -----

		// (rank average, rank evolution, win ratio, loss ratio, goals ratio, finals played, finals won, ratio)
		DataSet<Tuple7<String, Integer, Double, Integer, Double, Double, Double>> competitorsVectorsWithRanking;
		// (win ratio, loss ratio, goals ratio, finals played, finals won, ratio)
		DataSet<Tuple6<Double, Double, Double, Double, Double, Double>> competitorsVectorsWithoutRanking;







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
				.distinct();

		winners = env.readCsvFile(Settings.worldcupHistoryPath)
				.ignoreFirstLine()
				.types(Integer.class, String.class, String.class, String.class, String.class, String.class, Integer.class, Integer.class, Integer.class, Float.class)
				.flatMap(new WorldcupWinners());

		winnersVectorsWithRanking = fifaRanks.join(internationalResults)
				.where(0, 1)
				.equalTo(0, 1)
				.with(new JoinRanksAndResults())
				.join(worldcupHistory)
				.where(0, 1)
				.equalTo(0, 1)
				.with(new FinalistsVectorsWithRanking())
				.join(winners)
				.where(0, 1)
				.equalTo(0, 1)
				.with(new WinnersVectorsWithRanking());

		winnersVectorsWithoutRanking = internationalResults.join(worldcupHistory)
				.where(0, 1)
				.equalTo(0, 1)
				.with(new FinalistsVectorsWithoutRanking())
				.join(winners)
				.where(0, 1)
				.equalTo(0, 1)
				.with(new WinnersVectorsWithoutRanking());

		winnerVectorWithRanking = winnersVectorsWithRanking
				.reduceGroup(new WinnersVectorsWithRankingReduce());

		winnerVectorWithoutRanking = winnersVectorsWithoutRanking
				.reduceGroup(new WinnersVectorsWithoutRankingReduce());

		competitorsVectorsWithRanking = fifaRanks.join(internationalResults.filter(new FilterWorldcupEdition()))
				.where(0, 1)
				.equalTo(0, 1)
				.with(new JoinRanksAndResults());



		//internationalResults.print();
		//fifaRanks.print();
		//worldcupHistory.print();
		//winners.print();
		//winnersVectorsWithRanking.print();
		//winnerVectorWithRanking.print();
		//winnersVectorsWithoutRanking.print();
		//winnerVectorWithoutRanking.print();
		competitorsVectorsWithRanking.print();
	}

	// Winner vector with ranking (since 1994)
	// (0.49893470671731455,0.7290825086499529,0.07050037474835713,0.012528329017067418,0.07742714687235631,0.39121500464143816,0.23117250274266798,0.04348244694445422)

	// Winner vector without ranking (since 1930)
	// (0.2344814297436818,0.05998193940435631,0.25814814697962074,0.8069480544688464,0.45038961179656545,0.14409786686943687)

}
