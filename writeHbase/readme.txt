References
0.
HBase新版本与MapReduce集成
https://blog.csdn.net/tanggao1314/article/details/51439803
1.
spark将数据写入hbase以及从hbase读取数据
https://blog.csdn.net/u013468917/article/details/52822074
2.
HBae 之TableOutputFormat
http://blog.sina.com.cn/s/blog_6a67b5c501010jct.html
3.
[HBase] HBase 之TableOutputFormat
https://blog.csdn.net/mango_song/article/details/8314515
4.
HBase 之TableOutputFormat
http://blog.51cto.com/yaoyinjie/652188
5.
TableOutputFormat
https://so.csdn.net/so/search/s.do?q=TableOutputFormat&t=blog
6.
Spark2.1.1<spark写入Hbase的三种方法性能对比>
https://blog.csdn.net/gpwner/article/details/73530134
7.
Bulk Load－HBase数据导入最佳实践
https://blog.csdn.net/u011596455/article/details/77945998
8.
Spark实战之读写HBase
https://blog.csdn.net/u011812294/article/details/72553150
9.
MapReduce接口
https://www.cnblogs.com/warmingsun/p/6694251.html

HBase提供了TableInputFormat、TableOutputFormat、TableMapper和TableReducer类来支持使用MapReduce框架处理HBase上的数据，并提供了TableMapReduceUtil类来初始化一个HBase-MapReduce任务。下面介绍一下这些接口。
TableInputFormat类
TableInputFormat负责将HBase数据按Region进行切片，该类继承自TableInputFormatBase类，TableInputFormatBase类实现了InputFormat类的大部分功能，TableInputFormat只是在其上添加了几个配置接口。TableInputFormat类通过setConf接口进行配置。如果需要自定义HBase的InputFormat类，可以通过重载TableInputFormatBase类的方法进行开发。
TableOutputFormat类
TableOutputFormat类负责将MapReduce任务输出的数据写入HBase表中。TableOutputFormat类同样通过setConf方法进行配置，如通过设置 TableOutputFormat.OUTPUT_TABLE来设置输出的目标表格。
TableMapper类
TableMapper类是一个抽象类，继承自Mapper类
TableMapper输入的Key为RowKey的字节码数据，输入的Value为Result类型，表示一行数据。开发者需要重载TableMapper类的map方法来实现自己的Map任务。
TableReducer类
TableReducer类也是一个抽象类，继承自Reducer类