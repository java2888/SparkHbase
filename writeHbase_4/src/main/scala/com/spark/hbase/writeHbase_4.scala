package com.spark.hbase

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Administrator on 2018/8/16.
  */
object writeHbase_4 {
  def main(args: Array[String]): Unit = {
    val zookeeperQuorum = "hbase.zookeeper.quorum"
    val zookeeperCluster = "Master,Slave1,Slave2"
    val zookeeperClientPort = "hbase.zookeeper.property.clientPort"
    val zookeeperClientPort_value = "2181"
    val tableName = "t1"
    val AppName = "writeHbae_4"
    val columnFamily = "cf"
    val hbaseConf = HBaseConfiguration.create()
    hbaseConf.set(zookeeperQuorum, zookeeperCluster)
    hbaseConf.set(zookeeperClientPort, zookeeperClientPort_value)
    hbaseConf.set(TableOutputFormat.OUTPUT_TABLE, tableName)
    val job = Job.getInstance(hbaseConf)
    job.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])
    job.setOutputKeyClass(classOf[ImmutableBytesWritable])
    job.setOutputValueClass(classOf[Put])

    val sparkConf = new SparkConf().setAppName(AppName)
    val sc = new SparkContext(sparkConf)
    val arrayRDD = sc.makeRDD(Array("15,name15,88", "16,name16,44", "17,name17,22", "18,name18,100"))
    val rdd = arrayRDD.filter(line => {
      val arr = line.split(",")
      if (arr(2).toLong >= 100)
        false
      else
        true
    }
    ).map(line => line.split(",")).map(line => {
      val put = new Put(Bytes.toBytes(line(0)))
      put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("name"), Bytes.toBytes(line(1)))
      put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes("age"), Bytes.toBytes(line(2).toLong))
      (new ImmutableBytesWritable(), put)
    }
    )
    rdd.saveAsNewAPIHadoopDataset(job.getConfiguration())
    sc.stop()
  }
}
