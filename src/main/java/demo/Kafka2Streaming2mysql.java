package demo;

import dataframe.JdbcUtil;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import orm.domain.Person;
import scala.Function1;
import scala.Tuple2;
import scala.runtime.BoxedUnit;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;
import java.util.regex.Pattern;

/**
 * @author Clint
 * @company www.zbj.com
 * @comment
 * @date 2016-01-29.
 */
public class Kafka2Streaming2mysql {

    private static final String zk = "master:2181,slave1:2181,slave2:2181,slave3:2181,slave3:2181,slave4:2181";
    private static final String group="test-consumer-group";
    public Kafka2Streaming2mysql() {
    }

    public static void main(String[] args){

        SparkConf sparkConf = new SparkConf()
                .setMaster("spark://192.168.142.105:7077")
               // .setMaster("local")
                .setAppName("Kafka2Streaming2mysql").set("spark.cores.max", "5");
        JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(5));

        Map<String, Integer> topicMap = new HashMap<String, Integer>();
        topicMap.put("m0202", 1);
        JavaPairReceiverInputDStream<String, String> messages =
                KafkaUtils.createStream(jssc, zk, group, topicMap, StorageLevel.MEMORY_AND_DISK());

        JavaDStream<String> lines = messages.map(new Function<Tuple2<String, String>, String>() {
            @Override
            public String call(Tuple2<String, String> tuple2) {
                return tuple2._2();
            }
        });

        lines.foreachRDD(new Function<JavaRDD<String>, Void>() {
            @Override
            public Void call(JavaRDD<String> stringJavaRDD) throws Exception {

                SQLContext sqlContext = JavaSQLContextSingleton.getInstance(stringJavaRDD.context());
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

    /**保存数据到mysql 每个分区一个连接**/
      /*  lines.foreachRDD(new Function<JavaRDD<String>, Void>() {
            @Override
            public Void call(JavaRDD<String> stringJavaRDD) throws Exception {
                stringJavaRDD.foreachPartition(new VoidFunction<Iterator<String>>() {
                    Connection conn = DBUtils.getConnection();
                    @Override
                    public void call(Iterator<String> stringIterator) throws Exception {
                        while (stringIterator.hasNext()){
                            String[] data =   stringIterator.next().split(",");
                            Test t  = new Test(data[0],Integer.parseInt(data[1]));
                            String sql = "insert into test (systime,number) values(?,?)";
                            insertMed(conn,sql,t);
                        }
                    }
                });
                return null;
            }
        });*/


        /**保存数据到mysql 每条记录一个连接**/
        /*lines.foreachRDD(new Function<JavaRDD<String>, Void>() {
            @Override
            public Void call(JavaRDD<String> stringJavaRDD) throws Exception {

                stringJavaRDD.foreach(new VoidFunction<String>() {
                    @Override
                    public void call(String s) throws Exception {
                        System.out.println("####" +s);
                       String[] data=s.split(",");
                        Test t  = new Test(data[0],Integer.parseInt(data[1]));
                        Connection conn = DBUtils.getConnection();
                        String sql = "insert into test (systime,number) values(?,?)";
                        if(conn!=null){
                            insertMed(conn,sql,t);
                        }
                    }
                });
                return null;
            }
        });*/


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
class JavaSQLContextSingleton {
    private static transient SQLContext instance = null;
    public static SQLContext getInstance(SparkContext sparkContext) {
        if (instance == null) {
            instance = new SQLContext(sparkContext);
        }
        return instance;
    }
}


