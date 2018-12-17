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

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.api.java.tuple.Tuple9;
import org.apache.flink.util.Collector;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

public class BatchJob {

	static final String fifaRanksPath = "/Users/jordhanmadec/dev/INSA/world-cup-predictor/fifa_ranking.csv";
	static final String worldcupHistoryPath = "";
	static final String worldcupGamesPath = "";
	static final String internationalResultsPath = "/Users/jordhanmadec/dev/INSA/world-cup-predictor/international_results.csv";


	public static void main(String[] args) throws Exception {
		// set up the batch execution environment
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<Tuple6<Integer, String, Float, Float, Integer, Date>> fifaRanks = env.readCsvFile(fifaRanksPath)
				.ignoreFirstLine()
				.types(Integer.class, String.class, Float.class, Float.class, Integer.class, String.class)
				.flatMap(new FifaRankingDateConverter());

		DataSet<Tuple3<String, String, Integer>> internationalResults = env.readCsvFile(internationalResultsPath)
				.ignoreFirstLine()
				.types(String.class, String.class, String.class, Integer.class, Integer.class, String.class, String.class, String.class, Boolean.class)
				.flatMap(new InternationalResultsDateConverter())
				.flatMap(new InternationalResultsStats())
				.groupBy(0, 1)
				.sum(2);

		internationalResults.print();

		env.execute("Worldcup Predictor");
	}

	public static class FifaRankingDateConverter implements FlatMapFunction<Tuple6<Integer, String, Float, Float, Integer, String>, Tuple6<Integer, String, Float, Float, Integer, Date>> {
		@Override
		public void flatMap(Tuple6<Integer, String, Float, Float, Integer, String> in, Collector<Tuple6<Integer, String, Float, Float, Integer, Date>> out) throws Exception {
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
}
