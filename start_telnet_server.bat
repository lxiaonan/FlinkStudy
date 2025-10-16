@echo off
echo ====================================
echo Windows Socket服务器启动脚本
echo ====================================
echo.
echo 选择启动方式:
echo 1. Python Socket服务器 (推荐)
echo 2. PowerShell Socket服务器
echo 3. 使用telnet (需要先启用Windows telnet功能)
echo 4. 安装netcat for Windows
echo.
set /p choice=请选择 (1-4): 

if "%choice%"=="1" (
    echo 启动Python Socket服务器...
    python socket_server.py
) else if "%choice%"=="2" (
    echo 启动PowerShell Socket服务器...
    powershell -ExecutionPolicy Bypass -File socket_server.ps1
) else if "%choice%"=="3" (
    echo 使用telnet需要先启用Windows功能:
    echo 1. 打开"控制面板" - "程序" - "启用或关闭Windows功能"
    echo 2. 勾选"Telnet客户端"
    echo 3. 然后运行: telnet localhost 9999
    echo.
    echo 注意: telnet只能作为客户端，不能作为服务器
    echo 建议使用Python或PowerShell方案
    pause
) else if "%choice%"=="4" (
    echo 安装netcat for Windows:
    echo 1. 下载nmap (包含ncat): https://nmap.org/download.html
    echo 2. 安装后使用: ncat -l -k -p 9999
    echo 或者
    echo 1. 下载netcat for Windows: https://eternallybored.org/misc/netcat/
    echo 2. 解压后使用: nc -l -p 9999
    echo.
    pause
) else (
    echo 无效选择
    pause
)