package hbase

import conf.SparkConf
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.mapreduce.TableInputFormat

/**
  * Created by abilash on 10/6/17.
  */
object HBaseRead {
  def main(args:Array[String]): Unit ={
    val sparkContext = SparkConf.context;
    val hbaseConf = HBaseConf.conf

    // read
    val scan = new Scan
    scan.addColumn("cf".getBytes, "name".getBytes)

    hbaseConf.set(TableInputFormat.SCAN, HBaseUtils.convertScanToString(scan))
    val readTableName = "example_table"
    hbaseConf.set(TableInputFormat.INPUT_TABLE, readTableName)

    // Load an RDD of (ImmutableBytesWritable, Result) tuples from the table
    val hbaseRDD = sparkContext.newAPIHadoopRDD(hbaseConf, classOf[TableInputFormat],
      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
      classOf[org.apache.hadoop.hbase.client.Result])

    println("--------- count ----------"+hbaseRDD.count())
    //hbaseRDD.collect().foreach(println)
  }
}
