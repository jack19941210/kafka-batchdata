import random
import string
import time
import json
import socket
import struct
from confluent_kafka import Producer
from multiprocessing import Process, Queue

# Kafka配置
kafka_config = {
    'bootstrap.servers': '10.21.37.204:9092',
    'group.id': 'acct_to_redis_group',
    'default.topic.config': {'acks': 'all'}
}

topic = 'ACCT_TO_REDIS_TRACE'


# 生成随机IPv4地址
def random_ipv4():
    return socket.inet_ntoa(struct.pack('>I', random.randint(1, 0xffffffff)))


def random_ipv4_within_range():
    # 起始IP地址和允许的IP数量
    start_ip_str = '192.168.0.0'
    num_ips = 9000

    # 将起始IP地址转换为整数表示
    start_ip_int = struct.unpack('>I', socket.inet_aton(start_ip_str))[0]

    # 计算范围内的随机IP地址
    random_ip_int = start_ip_int + random.randint(0, num_ips - 1)

    # 将整数IP地址转换回字符串表示
    return socket.inet_ntoa(struct.pack('>I', random_ip_int))


# 生成随机IPv6地址
def random_ipv6():
    return ':'.join('%x' % random.randint(0, 2 ** 16 - 1) for _ in range(8))


# 生成随机MAC地址
def random_mac():
    return ':'.join(['%02x' % random.randint(0, 255) for _ in range(6)])


# 生成消息
def generate_message():
    nat_begin_port = random.randint(0, 65535)
    nat_end_port = random.randint(nat_begin_port, 65535)

    message = {
        "sessionId": ''.join(random.choices(string.ascii_letters + string.digits, k=32)),
        "userName": f"testliuyao{random.randint(1000, 9999)}",
        "bindAttr": "",
        "framedIp": random_ipv4(),
        "natIp": random_ipv4_within_range(),
        "natBeginPort": nat_begin_port,
        "natEndPort": nat_end_port,
        "framedIpv6": random_ipv6(),
        "ueIpv6Prefix": f"{random_ipv6()}/64",
        "nasIp": "58.240.161.253",
        "time": str(int(time.time() * 1000)),
        "pkgType": 3,
        "mac": random_mac()
    }
    return message


# 多进程发送消息到Kafka
def send_messages_worker(num_messages, queue, batch_size=2000):
    producer = Producer(kafka_config)
    batch = []
    total_sent = 0

    for _ in range(num_messages):
        message = generate_message()
        batch.append(json.dumps(message).encode('utf-8'))

        if len(batch) >= batch_size:
            for msg in batch:
                producer.produce(topic, msg)
            producer.flush()
            total_sent += len(batch)
            queue.put(len(batch))
            batch = []

    # 处理剩余的消息
    if batch:
        for msg in batch:
            producer.produce(topic, msg)
        producer.flush()
        total_sent += len(batch)
        queue.put(len(batch))

    producer.flush()


def monitor_progress(queue):
    total_messages = 0
    while True:
        count = queue.get()
        if count == 'DONE':
            break
        total_messages += count
        print(f"Total messages sent: {total_messages}")


def main():
    num_messages = 1000000
    num_workers = 4  # 设置进程数量
    messages_per_worker = num_messages // num_workers



    # 创建队列用于进程间通信
    queue = Queue()

    # 创建进度监控进程
    monitor_process = Process(target=monitor_progress, args=(queue,))
    monitor_process.start()

    # 记录开始时间
    start_time = time.time()

    # 创建多个发送消息的进程
    processes = []
    for _ in range(num_workers):
        p = Process(target=send_messages_worker, args=(messages_per_worker, queue))
        processes.append(p)
        p.start()

    # 等待所有进程完成
    for p in processes:
        p.join()

    # 发送结束信号
    queue.put('DONE')

    # 等待监控进程结束
    monitor_process.join()

    # 记录结束时间
    end_time = time.time()
    elapsed_time = end_time - start_time
    print(f"Total time taken: {elapsed_time} seconds")


if __name__ == "__main__":
    main()
