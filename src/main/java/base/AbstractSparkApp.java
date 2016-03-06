package base;/*
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


/**
 * @version 0.1
 *
 * @author Hefei Li
 *
 * @since Apr 9, 2015
 */

public abstract class AbstractSparkApp {

	/*argc(0) = "master:2181,slave1:2181,slave2:2181,slave3:2181,slave3:2181,slave4:2181"
	argc(1) = "test-consumer-group"
	argc(2) = "m0106"
	argc(3) = "1"
	val Array(zkQuorum, group, topics, numThreads) = argc
	val sparkConf = new SparkConf().setAppName("m0106mysql").setMaster("spark://192.168.142.105:7077")*/


	protected static final String SPARK_MASTER = "spark://192.168.142.105:7077";
}
