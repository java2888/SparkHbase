package com.spark.hbase

import org.apache.hadoop.hbase.{HBaseConfiguration, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, HBaseAdmin, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.{TableInputFormat, TableOutputFormat}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Administrator on 2018/8/12.
  */
object readHbase {
  def main(args: Array[String]): Unit = {
    val hbaseConf = HBaseConfiguration.create()
    val strZookeeperQuorum = "Master,Slave1,Slave2"
    val strZookeeperClientPort = "2181"
    val strTableName = "t1"
    hbaseConf.set("hbase.zookeeper.quorum", strZookeeperQuorum)
    hbaseConf.set("hbase.zookeeper.property.clientPort", strZookeeperClientPort)
    hbaseConf.set(TableInputFormat.INPUT_TABLE, strTableName)

    val connection = ConnectionFactory.createConnection(hbaseConf)
    val admin = connection.getAdmin()
    if (!admin.isTableAvailable(TableName.valueOf(strTableName))) {
      println("Warning: No table: " + strTableName)
      val tableDesc = new HTableDescriptor(TableName.valueOf(strTableName))
      admin.createTable(tableDesc)
    }

    val sparkConf = new SparkConf().setAppName("readHbase")
    val sc = new SparkContext(sparkConf)
    val hbaseRDD = sc.newAPIHadoopRDD(hbaseConf, classOf[TableInputFormat],
      classOf[ImmutableBytesWritable], classOf[Result])
    val count = hbaseRDD.count()
    println("count=" + count)
    //hbaseRDD.saveAsTextFile("hdfs://Master:9000/readHbase")
    var i = 0

    hbaseRDD.collect().foreach {
      //case (_, result)  => {
      line=> {
        val result=line._2
        i += 1
        println("i=" + i)
        val key = Bytes.toString(result.getRow())
        val name = Bytes.toString(result.getValue("cf".getBytes(), "name".getBytes()))
        if (result.getValue("cf".getBytes(), "age".getBytes()) == null)
          println("key=[" + key + "];  name=[" + name + "];  age=[null]")
        else {
          val age = Bytes.toLong(result.getValue("cf".getBytes(), "age".getBytes()))
          println("key=[" + key + "];  name=[" + name + "];  age=[" + age + "]")
        } //else
      } //case
    } //foreach
    sc.stop()
    admin.close()
  } //main
}

//readHbase
