References
1.
spark将数据写入hbase以及从hbase读取数据
从hbase读取数据转化成RDD
https://blog.csdn.net/u013468917/article/details/52822074
2.
HBase / HBASE-12083
Deprecate new HBaseAdmin() in favor of Connection.getAdmin()
https://issues.apache.org/jira/browse/HBASE-12083

While you are on all those modifcations, even if not related to current JIRA, might be nice to have something like
ConnectionFactory.createConnection();
Which create a connection with a default config.
A shortcut for
    Configuration conf = HBaseConfiguration.create();
    Connection connection = ConnectionFactory.createConnection(conf);
    HBaseAdmin admin = (HBaseAdmin)connection.getAdmin();

 * HBaseAdmin is no longer a client API. It is marked InterfaceAudience.Private indicating that
 * this is an HBase-internal class as defined in
 * https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-common/InterfaceClassification.html
 * There are no guarantees for backwards source / binary compatibility and methods or class can
 * change or go away without deprecation.
 * Use {@link Connection#getAdmin()} to obtain an instance of {@link Admin} instead of constructing
 * an HBaseAdmin directly.
3.
HBase / HBASE-12796
Clean up HTable and HBaseAdmin deprecated constructor usage
https://issues.apache.org/jira/browse/HBASE-12796
4.
HBase1.1.2增删改查scala代码实现
https://blog.csdn.net/sinat_35045195/article/details/76888583
5.
spark(2.1.0) 操作hbase(1.0.2)
https://www.cnblogs.com/runnerjack/p/7858241.html
6.
Spark 操作Hbase 对表的操作：增删改查 scala
https://blog.csdn.net/gongxuan92/article/details/43493595
7.
使用传统hbase的api创建hbase表（scala）
https://blog.csdn.net/high2011/article/details/52493823
8.
hbase shell操作之scan+filter
https://blog.csdn.net/liuxiao723846/article/details/73823056
