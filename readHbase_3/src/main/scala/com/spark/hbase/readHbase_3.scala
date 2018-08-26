package com.spark.hbase

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Administrator on 2018/8/21.
  */
object readHbase_3 {
  def main(args: Array[String]): Unit = {
    val zookeeperQuorum = "hbase.zookeeper.quorum"
    val zookeeperCluster = "Master,Slave1,Slave2"
    val zookeeperClientPort = "hbase.zookeeper.property.clientPort"
    val zookeeperClientPort_value = "2181"
    val tableName = "t1"
    val hbaseConf = HBaseConfiguration.create()
    hbaseConf.set(zookeeperQuorum, zookeeperCluster)
    hbaseConf.set(zookeeperClientPort, zookeeperClientPort_value)
    hbaseConf.set(TableInputFormat.INPUT_TABLE, tableName)
    val sparkConf = new SparkConf().setAppName("readHbase_3")
    val sc = new SparkContext(sparkConf)
    val tableRDD = sc.newAPIHadoopRDD(hbaseConf, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result])
    val count = tableRDD.count()
    println("count=" + count)
    val columnFamily = "cf"
    var i = 0
    val rdd = tableRDD.collect().foreach(line => {
      i += 1
      val result = line._2
      val key = Bytes.toString(result.getRow())
      val name = Bytes.toString(result.getValue(columnFamily.getBytes(), "name".getBytes()))
      val temp = result.getValue(columnFamily.getBytes(), "age".getBytes())
      if (temp == null) {
        println("i=" + i + "; key=" + key + "; name=" + name + "; age=null")
      } else {
        val age = Bytes.toLong(temp)
        println("i=" + i + "; key=" + key + "; name=" + name + "; age=" + age)
      }
    })
    sc.stop()
  }
}
