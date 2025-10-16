# Windows上模拟Linux nc -lk 9999操作指南

在Windows上为Flink SocketStreamWordCount程序提供socket数据源的多种方法。

## 问题背景

你的Flink程序 `SocketStreamWordCount.java` 中使用了：
```java
DataStreamSource<String> localhost = env.socketTextStream("localhost", 9999);
```

这需要在9999端口有一个socket服务器提供数据流。在Linux上通常使用 `nc -lk 9999`，但Windows需要其他方案。

## 解决方案

### 方案1: Python Socket服务器 (推荐)

**优点**: 跨平台、功能完整、易于定制

**使用方法**:
```bash
# 运行Python脚本
python socket_server.py
```

**特性**:
- 自动发送测试数据
- 支持手动输入数据
- 多客户端连接支持
- 详细的连接日志

### 方案2: PowerShell Socket服务器

**优点**: Windows原生、无需额外安装

**使用方法**:
```powershell
# 运行PowerShell脚本
powershell -ExecutionPolicy Bypass -File socket_server.ps1
```

**特性**:
- 纯PowerShell实现
- 彩色输出日志
- 交互式数据输入

### 方案3: 安装Netcat for Windows

**选项A: 使用Nmap中的ncat**
1. 下载安装Nmap: https://nmap.org/download.html
2. 安装后使用:
```bash
ncat -l -k -p 9999
```

**选项B: 独立的netcat**
1. 下载netcat for Windows: https://eternallybored.org/misc/netcat/
2. 解压到系统PATH或当前目录
3. 使用:
```bash
nc -l -p 9999
```

### 方案4: 使用telnet (限制较多)

**注意**: telnet主要是客户端工具，不适合作为服务器

**启用telnet客户端**:
1. 控制面板 → 程序 → 启用或关闭Windows功能
2. 勾选"Telnet客户端"
3. 确定并重启

**使用方法** (仅作为测试客户端):
```bash
telnet localhost 9999
```

## 快速启动

### 方法1: 使用批处理脚本
```bash
# 双击运行或在命令行执行
start_telnet_server.bat
```

### 方法2: 直接运行Python脚本
```bash
python socket_server.py
```

### 方法3: 直接运行PowerShell脚本
```powershell
powershell -ExecutionPolicy Bypass -File socket_server.ps1
```

## 测试流程

1. **启动socket服务器** (选择上述任一方案)
2. **运行Flink程序**:
   ```bash
   # 在另一个终端窗口
   mvn exec:java -Dexec.mainClass="com.study.SocketStreamWordCount"
   ```
3. **观察结果**: Flink程序会实时处理socket数据并输出词频统计

## 故障排除

### 端口被占用
```bash
# 检查端口占用
netstat -an | findstr :9999

# 杀死占用进程
taskkill /PID <进程ID> /F
```

### 防火墙问题
- 确保Windows防火墙允许Java和Python程序访问网络
- 或临时关闭防火墙进行测试

### 权限问题
- 以管理员身份运行命令提示符
- 或使用大于1024的端口号

## 推荐配置

**开发环境推荐**: Python方案
- 功能最完整
- 易于调试和修改
- 跨平台兼容

**生产环境推荐**: 根据实际需求选择合适的消息队列或流处理系统
- Apache Kafka
- Apache Pulsar
- RabbitMQ

## 文件说明

- `socket_server.py`: Python socket服务器实现
- `socket_server.ps1`: PowerShell socket服务器实现
- `start_telnet_server.bat`: 启动脚本，提供多种选择
- `Windows_Socket_Server_Guide.md`: 本说明文档