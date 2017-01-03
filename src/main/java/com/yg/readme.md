1.hadoop 小文件合并
##启动hadoop
hadoop jar data-analytics-1.0-SNAPSHOT.jar  com.yg.combinesmallfiles.combinesmallfilesbyhadoop.CombineSmallFilesDriver hdfs://quickstart.cloudera:8020/user/test/  hdfs://quickstart.cloudera:8020/user/test02/
