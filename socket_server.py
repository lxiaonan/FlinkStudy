#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Socket服务器脚本 - 模拟Linux nc -lk 9999命令
用于为Flink SocketStreamWordCount程序提供数据源

改进：
- 不在子线程中使用 input()，避免 Ctrl+C 后控制台卡死
- 主线程负责输入与广播，支持优雅退出
"""

import socket
import threading
import time
import sys
import signal
import argparse

# 维护客户端集合
clients = set()
clients_lock = threading.Lock()
stop_event = threading.Event()

def add_client(sock, addr):
    with clients_lock:
        clients.add((sock, addr))
    print(f"客户端 {addr} 已连接")

def remove_client(sock, addr):
    try:
        sock.close()
    except Exception:
        pass
    with clients_lock:
        clients.discard((sock, addr))
    print(f"客户端 {addr} 连接已关闭")

def broadcast(message: str):
    """向所有已连接客户端广播消息"""
    data = (message + "\n").encode("utf-8")
    to_remove = []
    with clients_lock:
        for sock, addr in list(clients):
            try:
                sock.sendall(data)
                print(f"发送到 {addr}: {message}")
            except Exception as e:
                print(f"发送到 {addr} 出错: {e}")
                to_remove.append((sock, addr))
    for sock, addr in to_remove:
        remove_client(sock, addr)

def accept_loop(server_socket: socket.socket):
    """接受客户端连接的线程"""
    while not stop_event.is_set():
        try:
            client_socket, addr = server_socket.accept()
            client_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            add_client(client_socket, addr)
        except socket.timeout:
            continue
        except OSError:
            # 套接字关闭后会触发，退出线程
            break
        except Exception as e:
            print(f"接受连接时出错: {e}")
            time.sleep(0.2)

def start_socket_server(host='localhost', port=9999):
    """启动Socket服务器"""
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

    try:
        server_socket.bind((host, port))
        server_socket.listen(5)
        server_socket.settimeout(1.0)  # 为优雅退出提供非阻塞 accept
        print(f"Socket服务器已启动，监听 {host}:{port}")
        print("等待Flink程序连接...")
        print("按Ctrl+C停止服务器")

        # 启动接受连接线程
        accept_thread = threading.Thread(target=accept_loop, args=(server_socket,), daemon=True)
        accept_thread.start()

        # 可选的预置数据（默认关闭）
        pre_send_data = [
            "s1,ppx",
            # "apache flink streaming",
            # "flink word count example",
            # "real time processing",
            # "big data analytics",
            # "stream processing engine"
        ]
        pre_send_enabled = False
        if pre_send_enabled:
            for data in pre_send_data:
                broadcast(data)
                time.sleep(1)

        # 主线程负责读取输入并广播
        print("\n现在可以手动输入数据，按Ctrl+C或输入 quit 退出:")
        while not stop_event.is_set():
            try:
                user_input = input("输入数据: ")
            except EOFError:
                # 管道/终端关闭
                break
            if user_input.strip().lower() == 'quit':
                break
            if user_input:
                broadcast(user_input)

    except KeyboardInterrupt:
        # 捕获 Ctrl+C，进行优雅关闭
        pass
    except Exception as e:
        print(f"启动服务器时出错: {e}")
    finally:
        # 触发停止事件，清理资源
        stop_event.set()
        print("\n正在关闭服务器...")
        try:
            server_socket.close()
        except Exception:
            pass
        # 关闭所有客户端连接
        with clients_lock:
            closing = list(clients)
        for sock, addr in closing:
            remove_client(sock, addr)
        print("服务器已关闭")

def _sigint_handler(sig, frame):
    # 统一处理 Ctrl+C
    stop_event.set()
    # 让 input() 尽快返回
    try:
        sys.stdout.write("\n")
        sys.stdout.flush()
    except Exception:
        pass

signal.signal(signal.SIGINT, _sigint_handler)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Socket服务器（模拟 nc -lk），支持命令行传参")
    parser.add_argument("port", nargs="?", type=int, default=9999, help="监听端口，默认 9999")
    parser.add_argument("host", nargs="?", default="localhost", help="绑定主机地址，默认 localhost")
    args = parser.parse_args()

    try:
        start_socket_server(host=args.host, port=args.port)
    except SystemExit:
        pass
    except KeyboardInterrupt:
        print("\n程序被用户中断")
        sys.exit(0)