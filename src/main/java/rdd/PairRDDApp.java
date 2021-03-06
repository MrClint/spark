/*
 * Copyright 2010-2011 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package rdd;

import base.AbstractSparkApp;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import rdd.tool.VideologFilter;
import rdd.tool.VideologPair;
import scala.Tuple2;
import com.google.common.base.Optional;

/**
 * @version 0.1
 *
 * @author Hefei Li
 *
 * @since Aug 25, 2015
 */

public class PairRDDApp extends AbstractSparkApp {
	private static final Logger log = LoggerFactory.getLogger(PairRDDApp.class);
	
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setMaster("local").setAppName("PairRDD");
		SparkContext sc = new SparkContext(conf);
		JavaSparkContext jsc = new JavaSparkContext(sc);

		JavaRDD<String> ipRepo = jsc.textFile("src/test/resources/data/IP_REPO_CN.txt").cache();
		JavaRDD<String> logs = jsc.textFile("src/test/resources/data/IP_LOGS.log").cache();
		
		JavaPairRDD<String, String> ipRepoPair = ipRepo.mapToPair(new PairFunction<String, String, String>() {
			@Override
			public Tuple2<String, String> call(String line) throws Exception {

				String ip = line.split("\t")[0].trim();
				
				return new Tuple2(ip, line.replaceAll(ip+"\t", ""));
			}
		}).distinct().cache();



		// collect logs by cat code and format time to time range.
		JavaRDD<VideologPair> logRdd = logs.map(new Function<String, VideologPair>() {
			@Override
			public VideologPair call(String line) throws Exception {
				VideologPair pair = VideologFilter.filte(line, "2015-07-02", "0000");
				
				return pair;
			}
		}).cache();
		
		JavaPairRDD<String, String> ipPair = logRdd.mapToPair(new PairFunction<VideologPair, String, String>() {

			@Override
			public Tuple2<String, String> call(VideologPair pair) throws Exception {
				
				return new Tuple2<String, String>(pair.getIp(), pair.getKey() +"\t"+ pair.getValue());
			}
			
		}).cache();
		
		//	
		JavaPairRDD<String, Tuple2<Optional<String>, String>> logsFullyRdd = ipPair
				.rightOuterJoin(ipRepoPair).cache();
		 
		JavaRDD<String> resultRdd = logsFullyRdd.map(new Function<Tuple2<String, Tuple2<Optional<String>, String>>, String>() {

			@Override
			public String call(
					Tuple2<String, Tuple2<Optional<String>, String>> val)
					throws Exception {


				Tuple2<Optional<String>, String> option = val._2();
				
				
				if(option._1().isPresent()){
					log.info("##############################option._1(): {}", option._1().get() +"\t"+ option._2());
				}

				
				return "";
			}
			
		}).cache();
		
		resultRdd.count();
		
		
		jsc.close();
		
	}

}
