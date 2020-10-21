
## Flink On Kubernetes Session集群部署
### 创建和启动Session集群
进入到session-mode路径下操作：
```bash
# 1.创建Flink Conf Configmap资源
kubectl create -f flink-configuration-configmap.yaml
# 2.创建JobManager Service
kubectl create -f jobmanager-service.yaml
# 3.创建JobManager和TaskManager Deployment
kubectl create -f jobmanager-session-deployment.yaml
kubectl create -f taskmanager-session-deployment.yaml
# 4.创建Restful服务，暴露Flink Web UI端口
kubectl create -f jobmanager-rest-service.yaml
```

### 提交任务到Flink Session集群
```bash
# 通过kubectl get svc 获取端口和地址信息
[root@node01 flink-training] kubectl get svc flink-jobmanager-rest
NAME                    TYPE       CLUSTER-IP     EXTERNAL-IP   PORT(S)          AGE
flink-jobmanager-rest   NodePort   10.96.174.29   <none>        8081:30084/TCP   13h
# 通过指定宿主机节点地址和端口提交Job
./bin/flink run -m node01:30084 ./examples/streaming/WordCount.jar
```
### 删除创建的组件和服务
```bash
kubectl delete -f jobmanager-service.yaml
kubectl delete -f flink-configuration-configmap.yaml
kubectl delete -f taskmanager-session-deployment.yaml
kubectl delete -f jobmanager-session-deployment.yaml
# if created then also the rest service
kubectl delete -f jobmanager-rest-service.yaml
# if created then also the queryable state service
kubectl delete -f taskmanager-query-state-service.yaml
```

## Flink On Kubunetes Per-Job模式部署
### 配置Application路径和参数
1. jobmanager-job-deployment.yaml配置文件修改
```yaml
containers:
        - name: jobmanager
          image: flink:1.11.1-scala_2.11
          env:
          args: ["standalone-job", "--job-classname", "org.apache.flink.streaming.examples.windowing.TopSpeedWindowing", --allowNonRestoredState, --output hdfs://node02:8020/flink-training/wordcount-output] 
          # optional arguments: ["--job-id", "<job id>", "--fromSavepoint", "/path/to/savepoint", "--allowNonRestoredState"]
          ports:
            - containerPort: 6123
              name: rpc
            - containerPort: 6124
              name: blob-server
            - containerPort: 8081
              name: webui
          livenessProbe:
            tcpSocket:
              port: 6123
            initialDelaySeconds: 30
            periodSeconds: 60
          volumeMounts:
            - name: flink-config-volume
              mountPath: /opt/flink/conf
            - name: job-artifacts-volume
              mountPath: /opt/flink/usrlib/
          securityContext:
            runAsUser: 9999  # refers to user _flink_ from official flink image, change if necessary
      volumes:
        - name: flink-config-volume
          configMap:
            name: flink-config-per-job
            items:
              - key: flink-conf.yaml
                path: flink-conf.yaml
              - key: log4j-console.properties
                path: log4j-console.properties
        - name: job-artifacts-volume
          hostPath:
            path: /home/flink-training/flink-1.11.1/examples/streaming/
```
2. taskmananger-job-deployment.yaml配置文件修改
```yaml
containers:
      - name: taskmanager
        image: flink:1.11.1-scala_2.11 #镜像可以自定义镜像
        env:
        args: ["taskmanager"] # 指定TaskManager类型实例
        ports:
        - containerPort: 6122
          name: rpc
        - containerPort: 6125
          name: query-state
        livenessProbe:
          tcpSocket:
            port: 6122
          initialDelaySeconds: 30
          periodSeconds: 60
        volumeMounts:
        - name: flink-config-volume #flink configuration 路径挂载
          mountPath: /opt/flink/conf/
        - name: job-artifacts-volume
          mountPath: /opt/flink/usrlib
        securityContext:
          runAsUser: 9999  # refers to user _flink_ from official flink image, change if necessary
      volumes:
      - name: flink-config-volume # 从configmap中获取
        configMap:
          name: flink-config-per-job
          items:
          - key: flink-conf.yaml
            path: flink-conf.yaml
          - key: log4j-console.properties
            path: log4j-console.properties
      - name: job-artifacts-volume # 指定宿主机路径
        hostPath:
          path: /home/flink-training/flink-1.11.1/examples/streaming/

```
### 创建和启动集群
进入到per-job-mode路径下操作：

#### 1.创建Flink Conf Configmap资源
```bash
kubectl create -f flink-configuration-configmap.yaml
```
#### 2.创建JobManager Service
```bash
kubectl create -f jobmanager-service.yaml
```
#### 3.创建JobManager和TaskManager Deployment
```bash
# 创建JobManager Deployment
kubectl create -f jobmanager-job-deployment.yaml
# 创建TaskManager Deployment
kubectl create -f taskmanager-job-deployment.yaml
```
#### 2.创建Restful服务，暴露Flink Web UI端口
```bash
kubectl create -f jobmanager-rest-service.yaml
```
### 提交任务到集群
该模式不支持提交任务

### 删除创建的组件和服务
```bash
kubectl delete -f jobmanager-service.yaml
kubectl delete -f flink-configuration-configmap.yaml
kubectl delete -f taskmanager-job-deployment.yaml
kubectl delete -f jobmanager-job-deployment.yaml
# if created then also the rest service
kubectl delete -f jobmanager-rest-service.yaml
# if created then also the queryable state service
kubectl delete -f taskmanager-query-state-service.yaml
```

## Flink On Kubernetes Native Mode部署

### 环境要求
- Kubernetes 1.9版本及以上
- Kubernetes集群具有授权认证，且获取到KubeConfig配置，通过该KubeConfig能够操作集群Pods资源；
- 开启Kubernetes DNS
- 创建支持RBAC权限管控的ServiceCount账号，可以支持create，删除pods资源；
### 配置修改
- 修改conf/flink-conf.yaml文件，增加KubeConfig配置
```yaml
kubernetes.config.file: /etc/kubernetes/admin.conf
```
### 启动Session集群
#### 1. 通过官方镜像启动

通过Flink安装路径./bin/kubernetes-session.sh脚本启动Session集群
```bash
$ ./bin/kubernetes-session.sh \
-Dkubernetes.cluster-id=flink-k8s-native-session-cluster \
-Dkubernetes.container.image=flink:latest \
-Djobmanager.heap.size=4096m \
-Dtaskmanager.memory.process.size=4096m \
-Dtaskmanager.numberOfTaskSlots=4 \
-Dkubernetes.jobmanager.cpu=1 \
-Dkubernetes.taskmanager.cpu=2 \
-Dkubernetes.namespace=default \
-Dkubernetes.jobmanager.service-account=flink
```
kubernetes.cluster-id如果不指定，则生成UUID

#### 2. 自定义镜像启动
```bash
$ ./bin/kubernetes-session.sh \
  -Dkubernetes.cluster-id=<ClusterId> \
  -Dtaskmanager.memory.process.size=4096m \
  -Dkubernetes.taskmanager.cpu=2 \
  -Dtaskmanager.numberOfTaskSlots=4 \
  -Dresourcemanager.taskmanager-timeout=3600000 \
  -Dkubernetes.container.image=<CustomImageName>
```


### 查看Flink Web UI
Flink支持多种方式暴露Flink Rest API服务以及UI，可以通过如下参数配置，选项有：ClusterIP,NodePort,LoadBalancer三种：
```
kubernetes.rest-service.exposed.type
```
- ClusterIP:直接使用Kubenetes集群内部IP，Flink JobManager服务只能在内部访问到，如果需要通过外部访问8081端口，则需要使用Local Proxy的方式，例如使用如下命令：
```
kubectl port-forward service/flink-k8s-native-session-cluster 8081
```
- NodePort:在所有节点Node上开放一个特定端口，任何发送到该端口的流量都被转发到对应服务。可以通过kubectl get svc 查询暴露出来的端口信息：
```bash
[root@node01 flink-training]# kubectl get svc
NAME                    TYPE       CLUSTER-IP     EXTERNAL-IP   PORT(S)          AGE
flink-jobmanager-rest   NodePort   10.96.174.29   <none>        8081:30081/TCP   15h
```
- LoadBalancer: 暴露服务到internet的标准方式,这种方式会启动一个 Network Load Balancer，它将给你一个单独的IP地址，转发所有流量到指定服务。
可以通过如下命令获取JobManager Service地址，然后配置Load balancer服务，并将地址暴露出来：
```
kubectl get services/flink-k8s-native-session-cluster 
```

### 提交job到指定Session集群
```bash 
$ ./bin/flink run -d -t kubernetes-session -Dkubernetes.cluster-id=flink-k8s-native-session-cluster examples/streaming/WindowJoin.jar
```

```
./bin/flink run -d -m node02:32762 -Dkubernetes.cluster-id=flink-k8s-native-session-cluster examples/streaming/WindowJoin.jar

```
也可使用上文暴露出来的IP:PORT提交作业，和Session集群模式一样；

### Attach到指定Session集群
```bash
$ ./bin/kubernetes-session.sh -Dkubernetes.cluster-id=<flink-k8s-native-session-cluster -Dexecution.attached=true
```
### 停止集群
```
$ echo 'stop' | ./bin/kubernetes-session.sh -Dkubernetes.cluster-id=flink-k8s-native-session-cluster -Dexecution.attached=true
```

## Flink On Kubernetes Application Mode部署
可以在Flink基础镜像上构建自定义镜像,DockerFile脚本如下：
```docker
FROM flink
RUN mkdir -p $FLINK_HOME/usrlib
COPY /path/of/my-flink-job-*.jar $FLINK_HOME/usrlib/my-flink-job.jar
```
启动Application应用
```base
$ ./bin/flink run-application -p 8 -t kubernetes-application \
  -Dkubernetes.cluster-id=flink-k8s-application-cluster \
  -Dtaskmanager.memory.process.size=4096m \
  -Dkubernetes.taskmanager.cpu=2 \
  -Dtaskmanager.numberOfTaskSlots=4 \
  -Dkubernetes.container.image=flink:latest \
  local:///opt/flink/examples/streaming/TopSpeedWindowing.jar
```
目前仅支持local模式，也就是指定镜像中的UserJar路径，需要用户事先将Jar打入到镜像中，hdfs等方式暂时不支持；


## 停止Application
```bash
./bin/flink cancel -t kubernetes-application -Dkubernetes.cluster-id=flink-k8s-application-cluster <JobID>
```