# 极客时间-Flink核心技术与实战
极客时间《Flink核心技术与实战》课程课件及代码

### 第一章：Apache Flink介绍
- 1.0 课程介绍
- 1.1 内容综述
- 1.2 流处理技术概览
- 1.2 Flink发展历史与应用场景 
- 1.4 Flink核心特性

### 第二章：Flink部署与应用
- 2.1 Flink集群架构（6）
- 2.2 Flink集群运行模式（7）
- 2.3 Flink集群资源管理器支持（8）
- 2.4 Standalone原理讲解与实操演示（9）
- 2.5 Flink On Yarn部署讲解（10）
- 2.6 Flink On Yarn实操演示（11）
- 2.7 Flink On Kubernetes部署讲解（12）
- 2.8 Flink On Kubernetes实操-Session模式（13）
- 2.9 Flink On Kubernetes实操-Per-Job模式14
- 3.0 Flink On Kubernetes Native部署讲解15
- 3.1 Flink On Kubernetes Native实操演示16
- 3.2 Flink高可用配置原理讲解17
- 3.3 Flink高可用配置实操演示18

### 第三章：Flink DataStream API实践原理
- 3.1 分布式流处理模型19
- 3.2 DataStream API 实践原理20
- 3.3 Flink 时间概念21
- 3.4 Watermark实践原理22
- 3.5 Watermark与Window的关系23
- 3.6 Watermark Generator（24）
- 3.7 Windows窗口计算（25）
- 3.8 Window Assigner（26）
- 3.9 Window Trigger（27）
- 3.10 Window Evictors（28）
- 3.11 Window Function
- 3.12 Windows多流合并
- 3.13 Process Function应用
- 3.14 SideOutput旁路输出
- 3.15 Asynchronous I/O异步操作
- 3.16 Pipeline与StreamGraph转换
- 3.17 项目实战：基于DataStream API实现PV，UV统计

### 第四章：Flink状态管理和容错
- 4.1 有状态计算概念
- 4.2 状态类型及应用
- 4.3 KeyedState与OperatorState
- 4.4 RuntimeContext与RichFunction实现
- 4.5 StateBackends状态管理器
- 4.6 状态序列化操作
- 4.7 Querable State介绍与使用
- 4.8 Checkpoints与Savepoints应用
- 4.9 Checkpoints实现原理
- 4.10 项目实战：基于状态计算实现用户画像指标统计

### 第五章：Flink Table & SQL实践原理
- 5.1 Flink Table API/SQL介绍与使用
- 5.2 TableEnviroment 原理与实践
- 5.3 TableConnector注册与实现
- 5.4 TimeStamp与Watermark原理实践
- 5.5 Temporal Tables原理实践
- 5.6 维表关联实践
- 5.7 Catalog原理与使用
- 5.8 Apache Hive集成
- 5.9 自定义Scalar/Table/AggregationFunction
- 5.10 自定义TableSource/TableSink/TableFactory
- 5.11 Flink SQL数据类型系统
- 5.12 项目实战：基于Flink SQL实现Top 10商品统计

### 第六章：Flink Runtime设计与实现
- 6.1 Runtime整体架构
- 6.2 ResourceManager资源管理
- 6.3 Execution Environment分类
- 6.4 JobGraph提交与运行
- 6.5 ExecutionGraph生成与执行
- 6.6 SchedulerNG调度器
- 6.7 Excution调度执行
- 6.8 Task重启策略与容错
- 6.9 StreamTask线程模型
- 6.10 集群高可用实现

### 第七章：Flink监控与性能优化
- 7.1 Metric指标分类与采集
- 7.2 Flink RestAPI介绍与使用
- 7.3 日志配置与问题定位
- 7.4 Checkpoint监控与调优
- 7.5 反压监控与原理
- 7.6 Flink内存配置与调优
- 7.7 窗口与事件时间调试
- 7.8 HistoryServer服务

### 第八章：Flink组件栈介绍与使用
- 8.1 PyFlink实践与应用
- 8.2 SQL Client实践与应用
- 8.3 Flink复杂事件处理：Complex event process
- 8.4 Flink图计算支持：Gelly graph process
- 8.5 Alink机器学习框架介绍与使用
- 8.6 Stateful Function介绍与使用

### 第九章：项目实战-使用Flink构建推荐系统实时数据流
- 9.1 实时推荐系统架构设计与实现
- 9.2 基于Flink SQL构建用户画像
- 9.3 基于Flink DataStream API样本归因与拼接
- 9.4 基于Elastic Search存储用户物料画像数据
- 9.5 推荐系统实时物料召回
- 9.6 结束语

### 代码编译

mvn clean package -DskipTests
