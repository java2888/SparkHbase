package com.spark.hbase

import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{ConnectionFactory, Put, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Administrator on 2018/8/20.
  */
object readHbase_2 {
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
    val connection = ConnectionFactory.createConnection(hbaseConf)
    val admin = connection.getAdmin()

/*    if (false == admin.isTableAvailable(TableName.valueOf(tableName))) {
      val tableDesc = admin.getTableDescriptor(TableName.valueOf(tableName))
      admin.createTable(tableDesc)
    } else {

    }*/
    val sparkConf = new SparkConf().setAppName("readHbase_2")
    val sc = new SparkContext(sparkConf)
    val initRDD = sc.newAPIHadoopRDD(hbaseConf, classOf[TableInputFormat], classOf[ImmutableBytesWritable],
      classOf[Result])
    val count = initRDD.count()
    println("count=" + count)
    var i = 0
    val rdd = initRDD.collect().foreach(line => {
      val result = line._2
      i = i + 1
      val key = Bytes.toString(result.getRow())
      val name = Bytes.toString(result.getValue(Bytes.toBytes("cf"), Bytes.toBytes("name")))
      if (result.getValue(Bytes.toBytes("cf"), Bytes.toBytes("age")) == null) {
        println("i=" + i + "; key=" + key + "; name=" + name + "; age=null")
      } else {
        val age = Bytes.toLong(result.getValue(Bytes.toBytes("cf"), Bytes.toBytes("age")))
        println("i=" + i + "; key=" + key + "; name=" + name + "; age=" + age)
      }
    })
    sc.stop()

  }
}
