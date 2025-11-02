#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Socket服务器脚本 - 模拟Linux nc -lk 9999命令
用于为Flink SocketStreamWordCount程序提供数据源
"""

import socket
import threading
import time
import sys

def handle_client(client_socket, addr):
    """处理客户端连接"""
    print(f"客户端 {addr} 已连接")
    
    try:
        # 发送一些测试数据
        test_data = [
            "s1,ppx",
#             "apache flink streaming",
#             "flink word count example",
#             "real time processing",
#             "big data analytics",
#             "stream processing engine"
        ]

        # 关闭前置发送
#         for data in test_data:
#             message = data + "\n"
#             client_socket.send(message.encode('utf-8'))
#             print(f"发送: {data}")
#             time.sleep(2)  # 每2秒发送一条数据
            
        # 保持连接，等待手动输入
        print("\n现在可以手动输入数据，按Ctrl+C退出:")
        while True:
            try:
                user_input = input("输入数据: ")
                if user_input.lower() == 'quit':
                    break
                message = user_input + "\n"
                client_socket.send(message.encode('utf-8'))
                print(f"发送: {user_input}")
            except EOFError:
                break
                
    except Exception as e:
        print(f"处理客户端时出错: {e}")
    finally:
        client_socket.close()
        print(f"客户端 {addr} 连接已关闭")

def start_socket_server(host='localhost', port=9999):
    """启动Socket服务器"""
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    
    try:
        server_socket.bind((host, port))
        server_socket.listen(5)
        print(f"Socket服务器已启动，监听 {host}:{port}")
        print("等待Flink程序连接...")
        print("按Ctrl+C停止服务器")
        
        while True:
            try:
                client_socket, addr = server_socket.accept()
                # 为每个客户端创建新线程
                client_thread = threading.Thread(
                    target=handle_client, 
                    args=(client_socket, addr)
                )
                client_thread.daemon = True
                client_thread.start()
                
            except KeyboardInterrupt:
                print("\n正在关闭服务器...")
                break
            except Exception as e:
                print(f"接受连接时出错: {e}")
                
    except Exception as e:
        print(f"启动服务器时出错: {e}")
    finally:
        server_socket.close()
        print("服务器已关闭")

if __name__ == "__main__":
    try:
        start_socket_server()
    except KeyboardInterrupt:
        print("\n程序被用户中断")
        sys.exit(0)