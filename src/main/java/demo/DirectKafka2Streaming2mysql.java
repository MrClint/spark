package demo;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;

import dataframe.JdbcUtil;
import kafka.serializer.StringDecoder;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaNewHadoopRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.kafka.HasOffsetRanges;
import scala.Tuple2;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.*;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.apache.spark.streaming.Durations;
import  org.apache.spark.streaming.kafka.KafkaCluster;
/**
 * @author Clint
 * @company www.zbj.com
 * @comment
 * @date 2016-02-02.
 */
public class DirectKafka2Streaming2mysql {

    private static final String zk = "master:2181,slave1:2181,slave2:2181,slave3:2181,slave3:2181,slave4:2181";
    private static final String group="test-consumer-group";
    public DirectKafka2Streaming2mysql() {
    }

    public static void main(String[] args){

        SparkConf sparkConf = new SparkConf()
                .setMaster("spark://192.168.142.105:7077")
                .setAppName("Kafka2Streaming2mysql").set("spark.cores.max", "5");
        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(5));

      //  Map<String, Integer> topicMap = new HashMap<String, Integer>();
      //  topicMap.put("m0202", 1);
        //JavaPairReceiverInputDStream<String, String> messages =   KafkaUtils.createStream(jssc, zk, group, topicMap, StorageLevel.MEMORY_AND_DISK());

        HashSet<String> topicsSet = new HashSet<String>(Arrays.asList("m0202"));

        HashMap<String, String> kafkaParams = new HashMap<String, String>();
        kafkaParams.put("metadata.broker.list", "master:9092,slave1:9092,slave2:9092,slave3:9092,slave4:9092");
        kafkaParams.put("group.id", "test-consumer-group");
        kafkaParams.put("auto.offset.reset", "largest");// 最新的数据
       // kafkaParams.put("auto.offset.reset", "smallest");

        JavaPairInputDStream<String, String> messages = KafkaUtils.createDirectStream(
                jssc,
                String.class,
                String.class,
                StringDecoder.class,
                StringDecoder.class,
                kafkaParams,
                topicsSet
        );
       /* messages.foreachRDD(new Function<JavaPairRDD<String, String>, Void>() {
            @Override
            public Void call(JavaPairRDD<String, String> stringStringJavaPairRDD) throws Exception {
                HasOffsetRanges hor = (HasOffsetRanges) stringStringJavaPairRDD;

                hor.offsetRanges();

               // KafkaCluster  kc = new KafkaCluster(kafkaParams);


                return null;
            }
        });*/




        JavaDStream<String> lines = messages.map(new Function<Tuple2<String, String>, String>() {
            @Override
            public String call(Tuple2<String, String> tuple2) {
                return tuple2._2();
            }
        });


        lines.foreachRDD(new Function<JavaRDD<String>, Void>() {
            @Override
            public Void call(JavaRDD<String> stringJavaRDD) throws Exception {

                SQLContext sqlContext =  JavaSQLContextSingleton2.getInstance(stringJavaRDD.context());

                JavaRDD<Test> data =  stringJavaRDD.map(new Function<String, Test>() {
                    @Override
                    public Test call(String s) throws Exception {
                        String[] data = s.split(",");
                        Test t = new Test(data[0], Integer.parseInt(data[2]));
                        return t;
                    }
                });

                DataFrame schemaTest = sqlContext.createDataFrame(data, Test.class);
                schemaTest.registerTempTable("testable");

                DataFrame wordCountsDataFrame =
                        sqlContext.sql("select systime, count(*) as total from testable group by systime");
                //wordCountsDataFrame.filter(wordCountsDataFrame.col("total").geq(1));
                wordCountsDataFrame.show();
                //wordCountsDataFrame.insertIntoJDBC();


                List<Test> list = null;
                list =  wordCountsDataFrame.javaRDD().map(new Function<Row, Test>() {
                    @Override
                    public Test call(Row row) throws Exception {
                        Test t = new Test(row.getString(0), Integer.parseInt(row.get(1).toString()));
                        return t;
                    }
                }).collect();
                Connection conn = DBUtils.getConnection();
                String sql = "insert into test (systime,number) values(?,?)";
                if(list!=null){
                    for(Test t :list){
                        insertMed(conn, sql,t);
                    }
                }

                return null;
            }
        });

        jssc.start();              // Start the computation
        jssc.awaitTermination();   // Wait for the computation to terminate

    }
    public static void insertMed(Connection conn, String sql, Test test){
        PreparedStatement ps = null;
        try {
            ps = conn.prepareStatement(sql);
            ps.setString(1,test.getSystime());
            ps.setInt(2, test.getNumber());
            ps.execute();
        } catch (SQLException e) {
            e.printStackTrace();
        }finally {
            DBUtils.closeResources(conn,ps,null);
        }
    }

}

/** Lazily instantiated singleton instance of SQLContext */
class JavaSQLContextSingleton2 {
    private static transient SQLContext instance = null;
    public static SQLContext getInstance(SparkContext sparkContext) {
        if (instance == null) {
            instance = new SQLContext(sparkContext);
        }
        return instance;
    }
}
