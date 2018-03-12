# 大数据相关计算练习

## hadoop

#### word count 执行
 ```
 hadoop jar hello-bigdata-1.0.0.jar player.data.hadoop.count.WordCountMain 
 ```
 
#### join reduce端连接
```
 hadoop jar hello-bigdata-1.0.0.jar player.data.hadoop.join.JoinMain 
 ```

## spark
```
spark-submit --class "player.data.spark.count.SimpleApp" --master spark://localhost:7077   hello-bigdata-1.0.0.jar 
```

## hbase
crud

## kylin
常用时间维度表的生成
