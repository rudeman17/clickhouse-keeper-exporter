import argparse
import socket
import time
import logging
import re
from prometheus_client import start_http_server, Gauge, Counter
from concurrent.futures import ThreadPoolExecutor, as_completed

class KeeperExporter:
    def __init__(self, hosts, timeout):
        self.hosts = hosts
        self.timeout = timeout
        self.metrics = {
            "ck_keeper_up": Gauge(
                "ck_keeper_up",
                "Whether the ClickHouse Keeper node is up (1) or down (0)",
                ["keeper_host"]
            ),
            "ck_keeper_leader": Gauge(
                "ck_keeper_leader",
                "Whether the Keeper node is leader (1) or not (0)",
                ["keeper_host"]
            ),
            "ck_keeper_latency_ms": Gauge(
                "ck_keeper_latency_ms",
                "Keeper latency in milliseconds",
                ["keeper_host", "type"]  # type: "avg", "max", "min"
            ),
            "ck_keeper_packets_total": Counter(
                "ck_keeper_packets_total",
                "Total packets received/sent",
                ["keeper_host", "direction"]  # direction: "received", "sent"
            ),
            "ck_keeper_outstanding_requests": Gauge(
                "ck_keeper_outstanding_requests",
                "Number of outstanding requests",
                ["keeper_host"]
            ),
            "ck_keeper_znode_count": Gauge(
                "ck_keeper_znode_count",
                "Number of znodes",
                ["keeper_host"]
            ),
            "ck_keeper_watch_count": Gauge(
                "ck_keeper_watch_count",
                "Number of watches",
                ["keeper_host"]
            ),
            "ck_keeper_ephemerals_count": Gauge(
                "ck_keeper_ephemerals_count",
                "Number of ephemeral nodes",
                ["keeper_host"]
            ),
            "ck_keeper_approximate_data_size": Gauge(
                "ck_keeper_approximate_data_size",
                "Approximate data size",
                ["keeper_host"]
            ),
            "ck_keeper_connections": Gauge(
                "ck_keeper_connections",
                "Number of connections",
                ["keeper_host"]
            ),
            "ck_keeper_followers": Gauge(
                "ck_keeper_followers",
                "Number of followers (for leader only)",
                ["keeper_host"]
            ),
            "ck_keeper_synced_followers": Gauge(
                "ck_keeper_synced_followers",
                "Number of synced followers (for leader only)",
                ["keeper_host"]
            ),
            "ck_keeper_key_arena_size": Gauge(
                "ck_keeper_key_arena_size",
                "Keeper key arena size",
                ["keeper_host"]
            ),
            "ck_keeper_latest_snapshot_size": Gauge(
                "ck_keeper_latest_snapshot_size",
                "Keeper latest snapshot size",
                ["keeper_host"]
            ),
            "ck_keeper_open_file_descriptor_count": Gauge(
                "ck_keeper_open_file_descriptor_count",
                "Keeper open file descriptor count",
                ["keeper_host"]
            ),
            "ck_keeper_max_file_descriptor_count": Gauge(
                "ck_keeper_max_file_descriptor_count",
                "Keeper max file descriptor count",
                ["keeper_host"]
            )
        }
    
    def send_keeper_cmd(self, host, port, cmd):
        try:
            with socket.create_connection((host, port), timeout=self.timeout) as s:
                s.sendall(cmd.encode())
                s.shutdown(socket.SHUT_WR)
                data = b''
                while True:
                    chunk = s.recv(4096)
                    if not chunk:
                        break
                    data += chunk
                logging.debug(f"Received data from {host}:{port} for cmd '{cmd}': {data!r}")
                return data.decode(errors='ignore')
        except Exception as e:
            logging.info(f"Failed to send cmd '{cmd}' to {host}:{port}: {e}")
            return None
    
    def parse_mntr_output(self, output):
        data = {}
        re_line = re.compile(r'zk_(\w+)\s+(.*)')
        for line in output.splitlines():
            m = re_line.match(line.strip())
            if m:
                key, value = m.group(1), m.group(2)
                try:
                    data[key] = float(value)
                except ValueError:
                    data[key] = value
        return data
    
    def handle_data(self, data, host):
        # 提取关键字段
        server_state = data.get("server_state", "unknown").lower()
        is_leader = 1 if server_state == "leader" else 0
        # print(data)
        # print("isleader: ", is_leader)
        # 设置 leader 指标
        self.metrics["ck_keeper_leader"].labels(keeper_host=host).set(is_leader)

        # znode 数量
        self.metrics["ck_keeper_znode_count"].labels(
            keeper_host=host
        ).set(data.get("znode_count", 0))

        # watch 数量
        self.metrics["ck_keeper_watch_count"].labels(
            keeper_host=host
        ).set(data.get("watch_count", 0))

        # 临时节点数量
        self.metrics["ck_keeper_ephemerals_count"].labels(
            keeper_host=host
        ).set(data.get("ephemerals_count", 0))

        # 数据大小
        self.metrics["ck_keeper_approximate_data_size"].labels(
            keeper_host=host
        ).set(data.get("approximate_data_size", 0))

        # 连接数
        self.metrics["ck_keeper_connections"].labels(
            keeper_host=host
        ).set(data.get("num_alive_connections", 0))

        # Latency
        self.metrics["ck_keeper_latency_ms"].labels(keeper_host=host, type="avg").set(
            data.get("avg_latency", 0)
        )
        self.metrics["ck_keeper_latency_ms"].labels(keeper_host=host, type="max").set(
            data.get("max_latency", 0)
        )
        self.metrics["ck_keeper_latency_ms"].labels(keeper_host=host, type="min").set(
            data.get("min_latency", 0)
        )

        # Packets
        self.metrics["ck_keeper_packets_total"].labels(keeper_host=host, direction="received").inc(
            data.get("packets_received", 0)
        )
        self.metrics["ck_keeper_packets_total"].labels(keeper_host=host, direction="sent").inc(
            data.get("packets_sent", 0)
        )

        # Outstanding requests
        self.metrics["ck_keeper_outstanding_requests"].labels(keeper_host=host).set(
            data.get("outstanding_requests", 0)
        )

        # 额外指标
        self.metrics["ck_keeper_key_arena_size"].labels(
            keeper_host=host
        ).set(data.get("key_arena_size", 0))

        self.metrics["ck_keeper_latest_snapshot_size"].labels(
            keeper_host=host
        ).set(data.get("latest_snapshot_size", 0))

        self.metrics["ck_keeper_open_file_descriptor_count"].labels(
            keeper_host=host
        ).set(data.get("open_file_descriptor_count", 0))

        self.metrics["ck_keeper_max_file_descriptor_count"].labels(
            keeper_host=host
        ).set(data.get("max_file_descriptor_count", 0))

        # Followers (only for leader)
        if is_leader:
            self.metrics["ck_keeper_followers"].labels(
                keeper_host=host
            ).set(data.get("followers", 0))
            self.metrics["ck_keeper_synced_followers"].labels(
                keeper_host=host
            ).set(data.get("synced_followers", 0))
        else:
            self.metrics["ck_keeper_followers"].labels(keeper_host=host).set(0)
            self.metrics["ck_keeper_synced_followers"].labels(keeper_host=host).set(0)

    def _collect_one(self, host):
        try:
            if ':' in host:
                h, p = host.split(':', 1)
                port = int(p)
            else:
                h = host
                port = 9181
            logging.debug(f"Collecting metrics from {h}:{port}")
            mntr = self.send_keeper_cmd(h, port, 'mntr')
            if mntr is None:
                self.metrics["ck_keeper_up"].labels(keeper_host=host).set(0)
                logging.warning(f"Failed to get mntr output from {h}:{port}")
                return
            data = self.parse_mntr_output(mntr)
            if not data:
                self.metrics["ck_keeper_up"].labels(keeper_host=host).set(0)
                logging.warning(f"No valid mntr data from {h}:{port}")
                return
            self.handle_data(data, host)
            self.metrics["ck_keeper_up"].labels(keeper_host=host).set(1)
        except Exception as e:
            logging.error(f"Exception collecting metrics from {host}: {e}")
            self.metrics["ck_keeper_up"].labels(keeper_host=host).set(0)
    def collect_metrics(self):
        with ThreadPoolExecutor(max_workers= min(10, len(self.hosts))) as executor:
            futures = [executor.submit(self._collect_one, host) for host in self.hosts]
            for future in as_completed(futures):
                pass 


def main():
    parser = argparse.ArgumentParser(description='ClickHouse Keeper Prometheus Exporter')
    parser.add_argument('--keeper-hosts', required=True, help='Comma separated list of keeper hosts, e.g. 127.0.0.1:9181,127.0.0.2:9181')
    parser.add_argument('--listen', default='0.0.0.0', help='Listen address')
    parser.add_argument('--port', type=int, default=9141, help='Exporter listen port')
    parser.add_argument('--timeout', type=int, default=10, help='Connection timeout (seconds)')
    parser.add_argument('--interval', type=int, default=30, help='Scrape interval (seconds)')
    parser.add_argument('--log-level', default='INFO', choices=['DEBUG', 'INFO'], help='Logging level')
    args = parser.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format='[%(asctime)s] %(levelname)s: %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    hosts = [h.strip() for h in args.keeper_hosts.split(',') if h.strip()]
    exporter = KeeperExporter(hosts, args.timeout)
    start_http_server(args.port, addr=args.listen)
    logging.info(f"Keeper exporter started at http://{args.listen}:{args.port}/")
    logging.info(f"Monitoring hosts: {hosts}")
    while True:
        exporter.collect_metrics()
        time.sleep(args.interval)

if __name__ == '__main__':
    main()
