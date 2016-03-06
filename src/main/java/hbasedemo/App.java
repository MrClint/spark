package hbasedemo;

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import kafka.serializer.StringDecoder;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import com.google.common.base.Optional;

import scala.Tuple2;


/**
 * Hello world!
 *
 */
public class App implements Serializable
{
    public static JavaPairRDD<String,Integer> transformFunc(JavaRDD<String> lines)
    {


        JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String,String>()
        {

            @Override
            public Iterable<String> call(String line) throws Exception {
                // TODO Auto-generated method stub
                String[] wordlist = line.split(" ");
                return Arrays.asList(wordlist);
            }

        });
        JavaPairRDD<String,Integer> mappedWords = words.mapToPair(new PairFunction<String,String,Integer>()
        {

            @Override
            public Tuple2<String, Integer> call(String word) throws Exception {
                // TODO Auto-generated method stub
                return new Tuple2(word,1);
            }

        });
        JavaPairRDD<String,Integer> wordcount = mappedWords.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {

                return v1 + v2;
            }
        });

        return wordcount;
    }


    public static void main( String[] args )
    {

        Class<? extends OutputFormat<?,?>> outputFormatClass = (Class<? extends
                OutputFormat<?,?>>) (Class<?>) TextOutputFormat.class;


        Map<String,String> kafkaParams = new HashMap<String,String>();
        kafkaParams.put("metadata.broker.list", "localhost:9092");
        Set<String> topics =  new HashSet<String>();
        topics.add("kafka-topic") ;

        SparkConf conf = new SparkConf().setAppName("FlumeSparkIntegration");

        JavaSparkContext jsc = new JavaSparkContext(conf);

        JavaStreamingContext jstc = new JavaStreamingContext(jsc, new Duration(10*1000));
        JavaPairInputDStream<String,String> kafkaStream =  KafkaUtils.createDirectStream(jstc, String.class, String.class, StringDecoder.class, StringDecoder.class, kafkaParams, topics);
        JavaDStream<String> line=kafkaStream.map(new Function<Tuple2<String,String>,String>()
        {

            @Override
            public String call(Tuple2<String, String> tuple2)
                    throws Exception {
                // TODO Auto-generated method stub
                return tuple2._2();

            }

        });

        JavaPairDStream<String,Integer> finalwordCount=line.transformToPair(new Function<JavaRDD<String>,JavaPairRDD<String,Integer>>()
        {

            @Override
            public JavaPairRDD<String, Integer> call(JavaRDD<String> lines)
                    throws Exception {
                // TODO Auto-generated method stub
                JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String,String>()
                {

                    @Override
                    public Iterable<String> call(String line) throws Exception {
                        // TODO Auto-generated method stub
                        String[] wordlist = line.split(" ");
                        return Arrays.asList(wordlist);
                    }

                });
                JavaPairRDD<String,Integer> mappedWords = words.mapToPair(new PairFunction<String,String,Integer>()
                {

                    @Override
                    public Tuple2<String, Integer> call(String word) throws Exception {
                        // TODO Auto-generated method stub
                        return new Tuple2(word,1);
                    }

                });
                JavaPairRDD<String,Integer> wordcount = mappedWords.reduceByKey(new Function2<Integer, Integer, Integer>() {
                    @Override
                    public Integer call(Integer v1, Integer v2) throws Exception {

                        return v1 + v2;
                    }
                });
                return wordcount;
            }

        });

        finalwordCount.toString();
        //finalwordCount.saveAsHadoopFiles("hdfs://localhost:54310/user/teach", "text",Text.class,IntWritable.class, outputFormatClass);


        jstc.start();
        jstc.awaitTermination();
    }
}