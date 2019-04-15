# spark-scala的运用

1.用的是maven来进行打包成jar包

![image](maven打包)

2.本地跑的scala，需要选择object
![image](scala_object)

3.如果需要用到本地测试的，则在测试中设计master

```
val logFile = "file:///F:/scala_workground/textllq/data/README.txt" // Should be some file on your system
val conf = new SparkConf().setAppName("Simple Application").setMaster("local[2]")
```

4.spark_test.scala：对rdd进行操作

5.spark_dataframe.scala:对dataframe进行操作