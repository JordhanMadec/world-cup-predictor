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

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.util.Collector;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

public class BatchJob {

	static final String fifaRanksPath = "/Users/jordhanmadec/dev/INSA/world-cup-predictor/fifa_ranking.csv";
	static final String worldcupHistoryPath = "";
	static final String worldcupGamesPath = "";
	static final String internationalGamesPath = "/Users/jordhanmadec/dev/INSA/world-cup-predictor/international_results.csv";


	public static void main(String[] args) throws Exception {
		// set up the batch execution environment
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<Tuple6<Integer, String, Float, Float, Integer, Date>> fifaRanks = env.readCsvFile(fifaRanksPath)
				.ignoreFirstLine()
				.types(Integer.class, String.class, Float.class, Float.class, Integer.class, String.class)
				.flatMap(new DateConverter());

		fifaRanks.print();

		env.execute("Flink Batch Java API Skeleton");
	}

	public static class DateConverter implements FlatMapFunction<Tuple6<Integer, String, Float, Float, Integer, String>, Tuple6<Integer, String, Float, Float, Integer, Date>> {

		@Override
		public void flatMap(Tuple6<Integer, String, Float, Float, Integer, String> in, Collector<Tuple6<Integer, String, Float, Float, Integer, Date>> out) throws Exception {
			DateFormat format = new SimpleDateFormat("yyyy-mm-dd");
			out.collect(new Tuple6<Integer, String, Float, Float, Integer, Date>(in.f0, in.f1, in.f2, in.f3, in.f4, format.parse(in.f5)));
		}
	}
}
