package com.spark.hbase

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{TableInputFormat, TableOutputFormat}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Administrator on 2018/8/22.
  */
object readHbase_4 {
  def main(args: Array[String]): Unit = {
    val zookeeperQuorum = "hbase.zookeeper.quorum"
    val zookeeperClusterHosts = "Master,Slave1,Slave2"
    val zookeeperClientPort = "hbase.zookeeper.property.clientPort"
    val zookeeperclientPort_value = "2181"
    val tableName = "t1"
    val appName = "readHbase_4"
    val hbaseConf = HBaseConfiguration.create()
    hbaseConf.set(zookeeperQuorum, zookeeperClusterHosts)
    hbaseConf.set(zookeeperClientPort, zookeeperclientPort_value)
    hbaseConf.set(TableInputFormat.INPUT_TABLE, tableName)
    val sparkConf = new SparkConf().setAppName(appName)
    val sc = new SparkContext(sparkConf)
    val hbaseTableRDD = sc.newAPIHadoopRDD(hbaseConf, classOf[TableInputFormat], classOf[ImmutableBytesWritable],
      classOf[Result])
    val count = hbaseTableRDD.count()
    println("count=" + count)
    val columFamily = "cf"
    var i = 0
    val rdd = hbaseTableRDD.collect().foreach(
      line => {
        i += 1
        val result = line._2
        val key = Bytes.toString(result.getRow)
        val name = Bytes.toString(result.getValue(columFamily.getBytes, "name".getBytes))
        val temp = result.getValue(columFamily.getBytes, "age".getBytes)
        if (temp == null) {
          println("i=" + i + "; name=" + name + "; age=null")
        } else {
          val age = Bytes.toLong(temp)
          println("i=" + i + "; name=" + name + "; age=" + age)
        } //if
      } //line=>{
    ) //foreach(
  } //main
} //object readHbase_4 {
