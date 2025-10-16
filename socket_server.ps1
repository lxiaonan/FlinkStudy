# PowerShell Socket服务器脚本
# 模拟Linux nc -lk 9999命令
# 用于为Flink SocketStreamWordCount程序提供数据源

param(
    [string]$Host = "localhost",
    [int]$Port = 9999
)

Write-Host "启动PowerShell Socket服务器..." -ForegroundColor Green
Write-Host "监听地址: $Host:$Port" -ForegroundColor Yellow
Write-Host "按Ctrl+C停止服务器" -ForegroundColor Yellow

try {
    # 创建TCP监听器
    $listener = New-Object System.Net.Sockets.TcpListener([System.Net.IPAddress]::Parse("127.0.0.1"), $Port)
    $listener.Start()
    
    Write-Host "等待Flink程序连接..." -ForegroundColor Cyan
    
    while ($true) {
        # 等待客户端连接
        $client = $listener.AcceptTcpClient()
        $stream = $client.GetStream()
        $writer = New-Object System.IO.StreamWriter($stream)
        
        Write-Host "客户端已连接: $($client.Client.RemoteEndPoint)" -ForegroundColor Green
        
        # 发送测试数据
        $testData = @(
            "hello world flink",
            "apache flink streaming", 
            "word count example",
            "real time processing",
            "big data analytics",
            "stream processing engine"
        )
        
        foreach ($data in $testData) {
            $writer.WriteLine($data)
            $writer.Flush()
            Write-Host "发送: $data" -ForegroundColor White
            Start-Sleep -Seconds 2
        }
        
        # 等待用户输入
        Write-Host "\n现在可以手动输入数据 (输入'quit'退出):" -ForegroundColor Yellow
        
        while ($true) {
            $userInput = Read-Host "输入数据"
            if ($userInput -eq "quit") {
                break
            }
            
            try {
                $writer.WriteLine($userInput)
                $writer.Flush()
                Write-Host "发送: $userInput" -ForegroundColor White
            }
            catch {
                Write-Host "客户端连接已断开" -ForegroundColor Red
                break
            }
        }
        
        # 清理资源
        $writer.Close()
        $stream.Close()
        $client.Close()
        Write-Host "客户端连接已关闭" -ForegroundColor Red
    }
}
catch {
    Write-Host "错误: $($_.Exception.Message)" -ForegroundColor Red
}
finally {
    if ($listener) {
        $listener.Stop()
        Write-Host "服务器已关闭" -ForegroundColor Red
    }
}