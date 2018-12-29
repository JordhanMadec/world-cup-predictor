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

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.*;

public class BatchJob {

	public static void main(String[] args) throws Exception {
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<Tuple4<String, Integer, Double, Integer>> fifaRanks;
		DataSet<Tuple5<String, Integer, Double, Double, Double>> internationalResults;
		DataSet<Tuple4<String, Integer, Integer, Double>> worldcupHistory;

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
				.reduceGroup(new WorldCupHistoryStatsReduce());

		//internationalResults.print();
		fifaRanks.print();
		//worldcupHistory.print();

		//env.execute("Worldcup Predictor");
	}









}
