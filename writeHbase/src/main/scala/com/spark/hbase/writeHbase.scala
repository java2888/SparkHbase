package com.spark.hbase

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Put, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.{SparkConf, SparkContext}


/**
  * Created by Administrator on 2018/8/8.
  */
object writeHbase {
  def main(args: Array[String]): Unit = {
    val zookeeperCluster = "Master,Slave1,Slave2"
    val zookeeperClientPort = "2181"
    val tableName = "t1"
    val hbaseConf = HBaseConfiguration.create()
    hbaseConf.set("hbase.zookeeper.quorum", zookeeperCluster)
    hbaseConf.set("hbase.zookeeper.property.clientPort", zookeeperClientPort)
    hbaseConf.set(TableOutputFormat.OUTPUT_TABLE, tableName)
    val job = Job.getInstance(hbaseConf)
    job.setOutputKeyClass(classOf[ImmutableBytesWritable])
    job.setOutputValueClass(classOf[Result])
    job.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])

    val sparkConf = new SparkConf().setAppName("writeHbase")
    val sc = new SparkContext(sparkConf)
    val initRDD = sc.makeRDD(Array("1,wang,20", "2,zhang,18", "3,li,70", "4,wang,100"))
    val rdd = initRDD.map(_.split(","))
      .map(line => {
        val put = new Put(Bytes.toBytes(line(0)))
        put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("name"), Bytes.toBytes(line(1)))
        put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("age"), Bytes.toBytes(line(2).toLong))
        (new ImmutableBytesWritable(), put)
      })
    rdd.saveAsNewAPIHadoopDataset(job.getConfiguration())
    sc.stop()

  }
}
