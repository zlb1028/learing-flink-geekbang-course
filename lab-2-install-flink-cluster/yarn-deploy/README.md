## Flink On Yarn部署说明
### 环境要求
 - Apache Hadoop 2.4.1及以上
 - HDFS (Hadoop Distributed File System) 环境
 - Hadoop依赖包

 ### 环境配置
 - 下载和解压安装包（参考Standalone模式）
 - 配置HADOOP_CONFIG_DIR环境变量

 ```bash
 $ vim /etc/profile
# 添加如下环境变量信息：
export JAVA_HOME=/usr/java/jdk1.8.0_241
export HADOOP_CONF_DIR=/home/flink-training/hadoop-conf
export HADOOP_CLASSPATH=/opt/cloudera/parcels/CDH/lib/hadoop:/opt/cloudera/parcels/CDH/lib/hadoop-yarn:/opt/cloudera/parcels/CDH/lib/hadoop-hdfs
export PATH=$PATH:$JAVA_HOME/bin
 ```
 - 如果HADOOP_CLASSPATH配置后，作业执行还报Hadoop依赖找不到错误，可以到如下地址下载，并放置在lib路径中：
 ```
$ cd lib/
$ wget https://repo.maven.apache.org/maven2/org/apache/flink/flink-shaded-hadoop-2-uber/2.7.5-10.0/flink-shaded-hadoop-2-uber-2.7.5-10.0.jar
```

### 基于Session Mode部署
```bash
./bin/yarn-session.sh -tm 1028 -s 8
```
可配置的运行参数如下：
```bash
Usage:
   Optional
     -D <arg>                        Dynamic properties
     -d,--detached                   Start detached
     -jm,--jobManagerMemory <arg>    Memory for JobManager Container with optional unit (default: MB)
     -nm,--name                      Set a custom name for the application on YARN
     -at,--applicationType           Set a custom application type on YARN
     -q,--query                      Display available YARN resources (memory, cores)
     -qu,--queue <arg>               Specify YARN queue.
     -s,--slots <arg>                Number of slots per TaskManager
     -tm,--taskManagerMemory <arg>   Memory per TaskManager Container with optional unit (default: MB)
     -z,--zookeeperNamespace <arg>   Namespace to create the Zookeeper sub-paths for HA mode
```

### Attach to an existing Session
```bash
$ ./bin/yarn-session.sh -id application_1463870264508_0029
```

### 提交Flink作业到指定Session集群
```bash
#上传测试文件
hdfs dfs -put ./data-set/frostroas.txt /flink-training
# 运行Flink程序
./bin/flink run ./examples/batch/WordCount.jar --input hdfs://node02:8020/flink-training/frostroad.txt --output hdfs://node02:8020/flink-training/wordcount-result.txt
```

### 停止集群服务
方式1：
```bash
echo "stop" | ./bin/yarn-session.sh -id application_1597152309776_0008
```
方式2：Yarn Kill命令
```bash
# 找到作业对应的ApplicationID
$ yarn application list
# 指定Kill命令
$ yarn application kill application_1597152309776_0008
```
### 基于Per-Job Mode部署
直接运行如下命令即可提交作业：
```bash
./bin/flink run -m yarn-cluster ./examples/batch/WordCount.jar
```
Detach 模式：
```bash
./bin/flink run -m yarn-cluster -d ./examples/batch/WordCount.jar
```

### 基于Application Mode部署

#### 1. 通过从本地上传Dependencies和User Application Jar
```bash
./bin/flink run-application -t yarn-application \
    -Djobmanager.memory.process.size=2048m \
    -Dtaskmanager.memory.process.size=4096m \
    ./MyApplication.jar
```
#### 2. 通过从HDFS获取Dependencies和本地上传User Application Jar
```bash
./bin/flink run-application -t yarn-application \
    -Djobmanager.memory.process.size=2048m \
    -Dtaskmanager.memory.process.size=4096m \
    -Dyarn.provided.lib.dirs="hdfs://node02:8020/flink-training/flink-1.11.1" \
    /home/flink-training/cluster-management/flink-on-yarn-1.11.1/examples/streaming/TopSpeedWindowing.jar
```

#### 3. 通过指定yarn.provided.lib.dirs参数部署，将Flink Binary包和Application Jar都同时从HDFS上获取
```bash
./bin/flink run-application -t yarn-application \
    -Djobmanager.memory.process.size=2048m \
    -Dtaskmanager.memory.process.size=4096m \
    -Dyarn.provided.lib.dirs="hdfs://node02:8020/flink-training/flink-1.11.1" \
    hdfs://node02:8020/flink-training/flink-1.11.1/examples/streaming/TopSpeedWindowing.jar
```

### 高可用配置