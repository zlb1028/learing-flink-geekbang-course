## Standalone 集群高可用配置
### 修改 conf/flink-conf.yaml:
high-availability: zookeeper
high-availability.zookeeper.quorum: node01:2181,node02:2181,node03:2181,node04:2181,node05:2181 
high-availability.zookeeper.path.root: /flink 
# high-availability.cluster-id: /cluster_one # important: customize per cluster 
high-availability.storageDir: hdfs:///flink/recovery
### 配置 conf/masters：
localhost:8081 
localhost:8082
### 配置 conf/zoo.cfg（可选）：
server.0=localhost:2888:3888
### 启动 HA 集群
$ bin/start-cluster.sh 
Starting HA cluster with 2 masters and 1 peers in ZooKeeper quorum. 
Starting standalonesession daemon on host localhost. 
Starting standalonesession daemon on host localhost. 
Starting taskexecutor daemon on host localhost.


### Flink On Yarn 集群高可用配置
### 修改 yarn-site.xml 配置，设定最大 Application Master 启动次数：
<property> 
	<name>yarn.resourcemanager.am.max-attempts</name>
	 <value>4</value>
	 <description> The maximum number of application master execution attempts. </description>
 </property>
### 修改配置文件 conf/flink-conf.yaml:
high-availability: zookeeper 
high-availability.zookeeper.quorum: localhost:2181 
high-availability.storageDir: hdfs:///flink/recovery 
high-availability.zookeeper.path.root: /flink 
yarn.application-attempts: 10
### Start an HA-cluster:
$ bin/yarn-session.sh -n 2
