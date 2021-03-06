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

package streaming;

import java.io.Serializable;
import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @version 0.1
 *
 * @author Hefei Li
 *
 * @since Apr 9, 2015
 */

public class BasicAvgApp {
	
	private static final Logger log = LoggerFactory.getLogger(BasicAvgApp.class);

	static class AvgFactor implements Serializable {
		private static final long serialVersionUID = 1L;
		
		public AvgFactor(Double total, int num) {
			this.num = num;
			this.total = total;
		}
		
		public double getAvg() {
			return total / num;
		}
		
		private Double total;
		private int    num;
	}

	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setMaster("local").setAppName("BasicAvgApp");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		JavaDoubleRDD factors = sc.parallelizeDoubles(Arrays.asList(2.2d, 789.78d, 98.99d, 0.87d, 184.789d));
		
		AvgFactor result = factors.aggregate(new AvgFactor(0d, 1), new Function2<AvgFactor, Double, AvgFactor>() {

			@Override
			public AvgFactor call(AvgFactor v1, Double v2) throws Exception {
				v1.total += v2;
				v1.num += 1;
				
				return v1;
			}
			
		}, new Function2<AvgFactor, AvgFactor, AvgFactor>(){

			@Override
			public AvgFactor call(AvgFactor v1, AvgFactor v2) throws Exception {
				v1.total += v2.total;
				v1.num += v2.num;
				return v1;
			}
			
		});
		
		log.info("Result: total = {}, num = {}, avg = {}", result.total, result.num, result.getAvg());
		
	}

}
