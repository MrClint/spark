package demo;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by Mike Mengarelli, Cloudera on 11/13/15
 *
 * Remedial Kafka Consumer using Spark Streaming
 *
 * Usage: PDCKafkaConsumer <zkQuorum> <group> <topics> <numThreads>
 *
 * From Gateway node Example:
 * spark-submit --class org.aspen.consumer.PIKafkaConsumer --deploy-mode client --master local[2]
 *   SparkStreamingKafkaConsumer.jar zk_host:2181 pigrp pikafkastream 1
 *
 * @TODO run on yarn in distributed mode
 */
public class PIKafkaConsumer {
    public static void main(String[] args) {
        /*if (args.length < 4) {
            System.err.println("Usage: PIKafkaConsumer <zkQuorum> <group> <topics> <numThreads>");
            System.exit(1);
        }*/

        String zkQuorum = "master:2181,slave1:2181,slave2:2181,slave3:2181,slave3:2181,slave4:218";
        String kfGrp = "test-consumer-group";
        String[] topics = new String[]{"m0127"};
       // int numThreads = Integer.valueOf(args[3]);
        int numThreads = 2;

        Map<String, Integer> topicMap = new HashMap<String, Integer>();
        for (String topic: topics) {
            topicMap.put(topic, numThreads);
        }

        SparkConf conf = new SparkConf().setAppName("PIKafkaConsumer");
       // conf.set("spark.ui.port","4030");
        JavaStreamingContext ctx = new JavaStreamingContext(conf, new Duration(10000));
        JavaPairReceiverInputDStream<String, String> kfStream = KafkaUtils.createStream(ctx, zkQuorum, kfGrp, topicMap);


       // kfStream.saveAsHadoopFiles("/pi/pi-data", "in", Text.class, Text.class, TextOutputFormat.class);

        ctx.start();
        ctx.awaitTermination();
    }
}