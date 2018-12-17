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

import javassist.bytecode.annotation.DoubleMemberValue;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.util.Collector;
import scala.Double$;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

public class BatchJob {

	static final String fifaRanksPath = "C:/Users/maxime  cadiou/Desktop/5INFO/Projet_big_data/projet/fifa_rankings.csv";
	static final String worldcupHistoryPath = "C:/Users/maxime  cadiou/Desktop/5INFO/Projet_big_data/projet/WorldCups.csv";
	static final String worldcupGamesPath = "";
	static final String internationalResultsPath = "C:/Users/maxime  cadiou/Desktop/5INFO/Projet_big_data/projet/international_results.csv";


	public static void main(String[] args) throws Exception {
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<Tuple2<String, Integer>> fifaRanks;
		DataSet<Tuple3<String, String, Integer>> internationalResults;

		fifaRanks = env.readCsvFile(fifaRanksPath)
				.ignoreFirstLine()
				.types(Integer.class, String.class, Float.class, Integer.class, Integer.class, String.class)
				.flatMap(new FifaRankingDateConverter())
				.flatMap(new FifaRankingStats())
				.groupBy(0)
				.reduceGroup(new FifaRankingReduce());

		internationalResults = env.readCsvFile(internationalResultsPath)
				.ignoreFirstLine()
				.types(String.class, String.class, String.class, Integer.class, Integer.class, String.class, String.class, String.class, Boolean.class)
				.flatMap(new InternationalResultsDateConverter())
				.flatMap(new InternationalResultsStats())
				.groupBy(0, 1)
				.sum(2);

		DataSet<Tuple3<String, String, Integer>> worldcupHistory = env.readCsvFile(worldcupHistoryPath)
				.ignoreFirstLine()
				.types(Integer.class, String.class, String.class, String.class, String.class, String.class, Integer.class, Integer.class, Integer.class, Integer.class)
				.flatMap(new worldcupHistoryStats())
				.groupBy(0)
				.sum(2);
		fifaRanks.print();

		//internationalResults.print();
		//fifaRanks.print();
		worldcupHistory.print();

		env.execute("Worldcup Predictor");
	}


	public static class worldcupHistoryStats implements FlatMapFunction<Tuple10<Integer, String, String, String, String, String, Integer, Integer, Integer, Integer>, Tuple3<String, String,  Integer>> {
		@Override
		public void flatMap(Tuple10<Integer, String, String, String, String, String, Integer, Integer, Integer, Integer> in, Collector<Tuple3<String, String, Integer>> out) throws Exception {

			out.collect(new Tuple3(in.f2, "vainqueur",  1));


		}
	}

	public static class FifaRankingDateConverter implements FlatMapFunction<Tuple6<Integer, String, Float, Integer, Integer, String>, Tuple6<Integer, String, Float, Integer, Integer, Date>> {
		@Override
		public void flatMap(Tuple6<Integer, String, Float, Integer, Integer, String> in, Collector<Tuple6<Integer, String, Float, Integer, Integer, Date>> out) throws Exception {
			DateFormat format = new SimpleDateFormat("yyyy-mm-dd");
			out.collect(new Tuple6(in.f0, in.f1, in.f2, in.f3, in.f4, format.parse(in.f5)));
		}
	}

	public static class InternationalResultsDateConverter implements FlatMapFunction<Tuple9<String, String, String, Integer, Integer, String, String, String, Boolean>, Tuple9<Date, String, String, Integer, Integer, String, String, String, Boolean>> {
		@Override
		public void flatMap(Tuple9<String, String, String, Integer, Integer, String, String, String, Boolean> in, Collector<Tuple9<Date, String, String, Integer, Integer, String, String, String, Boolean>> out) throws Exception {
			DateFormat format = new SimpleDateFormat("yyyy-mm-dd");
			out.collect(new Tuple9(format.parse(in.f0), in.f1, in.f2, in.f3, in.f4, in.f5, in.f6, in.f7, in.f8));
		}
	}

	public static class FifaRankingStats implements FlatMapFunction<Tuple6<Integer, String, Float, Integer, Integer, Date>, Tuple3<String, Integer, Integer>> {
		@Override
		public void flatMap(Tuple6<Integer, String, Float, Integer, Integer, Date> in, Collector<Tuple3<String, Integer, Integer>> out) throws Exception {
			out.collect(new Tuple3(in.f1, in.f0, 1));
		}
	}

	public static class InternationalResultsStats implements FlatMapFunction<Tuple9<Date, String, String, Integer, Integer, String, String, String, Boolean>, Tuple3<String, String, Integer>> {
		@Override
		public void flatMap(Tuple9<Date, String, String, Integer, Integer, String, String, String, Boolean> in, Collector<Tuple3<String, String, Integer>> out) throws Exception {
			if (in.f3 > in.f4) {
				out.collect(new Tuple3(in.f1, "win", 1));
				out.collect(new Tuple3(in.f2, "loss", 1));
				if (in.f5.equals("FIFA World Cup")) {
					out.collect(new Tuple3(in.f1, "wc_win", 1));
					out.collect(new Tuple3(in.f2, "wc_loss", 1));
				}
			} else if (in.f3 < in.f4) {
				out.collect(new Tuple3(in.f2, "win", 1));
				out.collect(new Tuple3(in.f1, "loss", 1));
				if (in.f5.equals("FIFA World Cup")) {
					out.collect(new Tuple3(in.f2, "wc_win", 1));
					out.collect(new Tuple3(in.f1, "wc_loss", 1));
				}
			} else {
				out.collect(new Tuple3(in.f1, "draw", 1));
				out.collect(new Tuple3(in.f2, "draw", 1));
				if (in.f5.equals("FIFA World Cup")) {
					out.collect(new Tuple3(in.f1, "wc_draw", 1));
					out.collect(new Tuple3(in.f2, "wc_draw", 1));
				}
			}

			out.collect(new Tuple3(in.f1, "goals_for", in.f3));
			out.collect(new Tuple3(in.f2, "goals_for", in.f4));
			out.collect(new Tuple3(in.f1, "goals_against", in.f4));
			out.collect(new Tuple3(in.f2, "goals_against", in.f3));

			if (in.f5.equals("FIFA World Cup")) {
				out.collect(new Tuple3(in.f1, "wc_goals_for", in.f3));
				out.collect(new Tuple3(in.f2, "wc_goals_for", in.f4));
				out.collect(new Tuple3(in.f1, "wc_goals_against", in.f4));
				out.collect(new Tuple3(in.f2, "wc_goals_against", in.f3));
			}
		}
	}




	public static class FifaRankingReduce
			implements GroupReduceFunction<Tuple3<String, Integer, Integer>, Tuple2<String, Integer>> {

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
}
