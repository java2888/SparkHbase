package com.spark.hbase

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Put, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Administrator on 2018/8/15.
  */
object writeHbase_2 {
  def main(args: Array[String]): Unit = {
    val hbaseConf = HBaseConfiguration.create()
    val zookeeperQuorum = "hbase.zookeeper.quorum"
    val zookeeperQuorumCluster = "Master,Slave1,Slave2"
    val zookeeperClientPort = "hbase.zookeeper.property.clientPort"
    val zookeeperClinetPort_Value = "2181"
    val tableName = "t1"
    hbaseConf.set(zookeeperQuorum, zookeeperQuorumCluster)
    hbaseConf.set(zookeeperClientPort, zookeeperClinetPort_Value)
    hbaseConf.set(TableOutputFormat.OUTPUT_TABLE, tableName)
    val job = Job.getInstance(hbaseConf)
    job.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])
    job.setOutputKeyClass(classOf[ImmutableBytesWritable])
    job.setOutputValueClass(classOf[Result])
    val sparkConf=new SparkConf().setAppName("writeHbase_2")
    val sc=new SparkContext(sparkConf)
    val arrayRDD=sc.makeRDD(Array("10,name10,66","11,name11,88","12,name12,99"))
    val columnFamily="cf"
    val rdd=arrayRDD.map(line=>line.split(",")).map(
      line=>{
        val put=new Put(Bytes.toBytes(line(0)))
        put.addColumn(Bytes.toBytes(columnFamily),Bytes.toBytes("name"),Bytes.toBytes(line(1)))
        put.addColumn(Bytes.toBytes(columnFamily),Bytes.toBytes("age"),Bytes.toBytes(line(2).toLong))
        (new ImmutableBytesWritable(),put)
      }
    )
    rdd.saveAsNewAPIHadoopDataset(job.getConfiguration())
    sc.stop()
  }
}
