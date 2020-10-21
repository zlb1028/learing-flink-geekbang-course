## 项目说明

## Kafka 配置

$KAFKA_HOME/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic user_behavior

## ES 配置安装


## WebSocket 安装文档
1. 安装node,参考文档：https://nodejs.org/en/ 

2. 安装ws模块，ws：是nodejs的一个WebSocket库，可以用来创建服务。 https://github.com/websockets/ws

    ```bash
    npm install ws
    ```
3. 进入到learning-flink/realtime-monitor-project/src/main/resources/websocket路径中；

4. 执行如下命令，启动We bSocketServer：
    ```bash
    node index.js
    ```
5. 使用浏览器打开webSocket.html文件，确认浏览器中显示已经连接字段；