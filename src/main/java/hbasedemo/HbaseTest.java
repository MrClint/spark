package hbasedemo;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.util.Base64;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;

import scala.Tuple2;

public class HbaseTest {

    // private static String master = "spark://slave01.infobird.com:7077";

    public static void main(String[] args) {

        SparkConf sparkConf = new SparkConf().setAppName("HbaseTest")
                .setMaster("spark://slave01.infobird.com:7077");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        try {

            testSparkCache(sc);
            //testSparkHbase(sc);

        } catch (Exception e) {

            e.printStackTrace();

        } finally {
            if (sc != null) {
                sc.close();
            }
        }
    }

    public static void testSparkCache(JavaSparkContext sc) throws IOException {
        Configuration conf = HBaseConfiguration.create();

        Scan scan = new Scan();

        scan.addFamily(Bytes.toBytes("baseinfo"));

        scan.addColumn(Bytes.toBytes("baseinfo"), Bytes.toBytes("city"));

        String tableName = "call_info_history";

        conf.set(TableInputFormat.INPUT_TABLE, tableName);

        ClientProtos.Scan proto = ProtobufUtil.toScan(scan);

        String ScanToString = Base64.encodeBytes(proto.toByteArray());

        conf.set(TableInputFormat.SCAN, ScanToString);

        JavaPairRDD<ImmutableBytesWritable, Result> myRDD = sc
                .newAPIHadoopRDD(conf, TableInputFormat.class,
                        ImmutableBytesWritable.class, Result.class);


        //JavaPairRDD<ImmutableBytesWritable, Result> myRDD1 = myRDD.cache();
        Integer r = myRDD.map(new Function<Tuple2<ImmutableBytesWritable, Result>, Integer>() {

            private static final long serialVersionUID = 1L;

            @Override
            public Integer call(Tuple2<ImmutableBytesWritable, Result> v1)
                    throws Exception {

                System.out.println("=============begion print===================");

                System.out.println("age:" + v1._2().getValue(Bytes.toBytes("baseinfo"), Bytes.toBytes("city")));

                List<Cell> keyvalus = v1._2().listCells();
                for (Cell cell : keyvalus) {
                    System.out.println("key:"  + new String(cell.getRowArray(),
                            cell.getRowOffset(), cell.getRowLength()));
                    System.out.println("family:"  + new String(cell.getFamilyArray(),
                            cell.getFamilyOffset(), cell.getFamilyLength()));
                    System.out.println("qualifier:"  + new String(cell.getQualifierArray(),
                            cell.getQualifierOffset(), cell.getQualifierLength()));
                    System.out.println("value:"  + new String(cell.getValueArray(),
                            cell.getValueOffset(), cell.getValueLength()));
                }

                return 1;
            }
        }).reduce(new Function2<Integer, Integer, Integer>() {

            private static final long serialVersionUID = 1L;

            @Override
            public Integer call(Integer integer, Integer integer2) {
                return integer + integer2;
            }
        });
        System.out.println("======resulut : ======:" +r);
        System.out.println("======myRDD.count() : ======:" + myRDD.count());
    }

    public static void testSparkHbase(JavaSparkContext sc) throws IOException {

        Configuration conf = HBaseConfiguration.create();

        Scan scan = new Scan();

        scan.addFamily(Bytes.toBytes("baseinfo"));

        scan.addColumn(Bytes.toBytes("baseinfo"), Bytes.toBytes("city"));

        String tableName = "call_info_history";

        conf.set(TableInputFormat.INPUT_TABLE, tableName);

        ClientProtos.Scan proto = ProtobufUtil.toScan(scan);

        String ScanToString = Base64.encodeBytes(proto.toByteArray());

        conf.set(TableInputFormat.SCAN, ScanToString);

        JavaPairRDD<ImmutableBytesWritable, Result> myRDD = sc
                .newAPIHadoopRDD(conf, TableInputFormat.class,
                        ImmutableBytesWritable.class, Result.class);


        Integer r = myRDD.map(new Function<Tuple2<ImmutableBytesWritable, Result>, Integer>() {

            private static final long serialVersionUID = 1L;

            @Override
            public Integer call(Tuple2<ImmutableBytesWritable, Result> v1)
                    throws Exception {

                System.out.println("=============begion print===================");

                System.out.println("age:" + v1._2().getValue(Bytes.toBytes("baseinfo"), Bytes.toBytes("city")));

                List<Cell> keyvalus = v1._2().listCells();
                for (Cell cell : keyvalus) {
                    System.out.println("key:"  + new String(cell.getRowArray(),
                            cell.getRowOffset(), cell.getRowLength()));
                    System.out.println("family:"  + new String(cell.getFamilyArray(),
                            cell.getFamilyOffset(), cell.getFamilyLength()));
                    System.out.println("qualifier:"  + new String(cell.getQualifierArray(),
                            cell.getQualifierOffset(), cell.getQualifierLength()));
                    System.out.println("value:"  + new String(cell.getValueArray(),
                            cell.getValueOffset(), cell.getValueLength()));
                }

                return 1;
            }
        }).reduce(new Function2<Integer, Integer, Integer>() {

            private static final long serialVersionUID = 1L;

            @Override
            public Integer call(Integer integer, Integer integer2) {
                return integer + integer2;
            }
        });
        System.out.println("======resulut : ======:" +r);
        System.out.println("======myRDD.count() : ======:" + myRDD.count());
    }
}