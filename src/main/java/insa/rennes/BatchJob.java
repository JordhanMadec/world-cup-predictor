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

import insa.rennes.fifaRanking.FifaRankingDateConverter;
import insa.rennes.fifaRanking.FifaRankingStats;
import insa.rennes.fifaRanking.FifaRankingStatsReduce;
import insa.rennes.internationalResults.InternationalResultsDateConverter;
import insa.rennes.internationalResults.InternationalResultsStats;
import insa.rennes.internationalResults.InternationalResultsStatsReduce;
import insa.rennes.vectors.*;
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





		// (team, edition, rank average, rank evolution)
		DataSet<Tuple4<String, Integer, Double, Integer>> fifaRanks;

		// (team, edition, win ratio, loss ratio, goals ratio)
		DataSet<Tuple5<String, Integer, Double, Double, Double>> internationalResults;

		// (team, edition, finals played, finals won, ratio)
		DataSet<Tuple5<String, Integer, Integer, Integer, Double>> worldcupHistory;

		// (team, edition)
		DataSet<Tuple2<String, Integer>> winners;

		// (team, edition, rank average, rank evolution, win ratio, loss ratio, goals ratio)
		DataSet<Tuple7<String, Integer, Double, Integer, Double, Double, Double>> vectors;

		// (team, edition, rank average, rank evolution, win ratio, loss ratio, goals ratio, finals played, finals won, ratio)
		DataSet<Tuple10<String, Integer, Double, Integer, Double, Double, Double, Integer, Integer, Double>> winnersVectorsWithRanking;
		// (team, edition, win ratio, loss ratio, goals ratio, finals played, finals won, ratio)
		DataSet<Tuple8<String, Integer, Double, Double, Double, Integer, Integer, Double>> winnersVectorsWithoutRanking;

		// (rank average, rank evolution, win ratio, loss ratio, goals ratio, finals played, finals won, ratio)
		DataSet<Tuple8<Double, Double, Double, Double, Double, Double, Double, Double>> winnerVectorWithRanking;
		// (win ratio, loss ratio, goals ratio, finals played, finals won, ratio)
		DataSet<Tuple6<Double, Double, Double, Double, Double, Double>> winnerVectorWithoutRanking;






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

		vectors = fifaRanks.join(internationalResults)
				.where(0, 1)
				.equalTo(0, 1)
				.with(new JoinRanksAndResults());

		winnersVectorsWithRanking = vectors.join(worldcupHistory)
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



		//internationalResults.print();
		//fifaRanks.print();
		//worldcupHistory.print();
		//vectors.print();
		//vectors.print();
		//winnersVectorsWithRanking.print();
		//winnerVectorWithRanking.print();
		//winnersVectorsWithoutRanking.print();
		//winnerVectorWithoutRanking.print();
	}

	// Winner vector with ranking (since 1994)
	// (4.676270685579197,6.833333333333333,0.6607654895911642,0.11742189295120946,0.7256867327779943,3.6666666666666665,2.1666666666666665,0.4075396825396825)

	// Winner vector without ranking (since 1930)
	// (0.6247429077460883,0.159813471270157,0.6877995590081843,2.15,1.2,0.3839285714285714)

}
