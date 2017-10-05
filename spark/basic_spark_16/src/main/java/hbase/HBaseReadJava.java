package hbase;

import conf.SparkUtilJava;
import hbase.models.KeyValueData;
import hbase.utils.HBaseUtilJava;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import scala.Tuple2;

/**
 * Created by Abilash George Thomas on 25/9/17.
 */
public class HBaseReadJava {

    public static void main(String[] args) throws Exception {
        JavaSparkContext sparkContext = SparkUtilJava.getSparkContext();

        Configuration hbaseConfig = HBaseUtilJava.connection("example_table");

        Scan scan = new Scan();
        scan.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("name"));
        hbaseConfig.set(TableInputFormat.SCAN, HBaseUtilJava.convertScanToString(scan));

        JavaPairRDD<ImmutableBytesWritable, Result> inputBaseRDD = sparkContext.newAPIHadoopRDD(
                hbaseConfig,
                TableInputFormat.class,
                ImmutableBytesWritable.class, Result.class);

        JavaRDD<KeyValueData> resultRdd =  inputBaseRDD.map(new Function<Tuple2<ImmutableBytesWritable,Result>, Result>() {
            //@Override
            public Result call(Tuple2<ImmutableBytesWritable, Result> v1) throws Exception {
                return v1._2;
            }
        }).filter(new Function<Result, Boolean>() {

            //@Override
            public Boolean call(Result result) throws Exception {
                return true;
            }
        }).map(new Function<Result, KeyValueData>() {
            //@Override
            public KeyValueData call(Result result) throws Exception {
                return new KeyValueData(Bytes.toString(result.getValue("cf".getBytes(), "name".getBytes())), Bytes.toString(result.getValue("cf".getBytes(), "name".getBytes())));
            }
        });

        System.out.println("---------" + resultRdd.count());
    }

}
