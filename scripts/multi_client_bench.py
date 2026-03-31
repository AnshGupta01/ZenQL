import socket
import threading
import time
import random
import sys

# FlexQL Server Configuration
HOST = '127.0.0.1'
PORT = 9000
NUM_CLIENTS = 8
OPS_PER_CLIENT = 1000

# SQL Templates
INSERT_SQL = "INSERT INTO BENCH_TABLE VALUES ({id}, 'user{id}', {balance}, 1893456000);"
SELECT_SQL = "SELECT * FROM BENCH_TABLE WHERE ID = {id};"

def encode_frame(sql):
    sql_bytes = sql.encode('utf-8')
    length = len(sql_bytes).to_bytes(4, byteorder='little')
    sentinel = b'\xef\xbe\xad\xde' # 0xDEADBEEF in little-endian
    return length + sql_bytes + sentinel

def decode_frame(sock):
    length_bytes = sock.recv(4)
    if not length_bytes: return None
    length = int.from_bytes(length_bytes, byteorder='little')
    payload = b""
    while len(payload) < length:
        chunk = sock.recv(length - len(payload))
        if not chunk: break
        payload += chunk
    sentinel = sock.recv(4) # Consume sentinel
    return payload.decode('utf-8')

class Client(threading.Thread):
    def __init__(self, client_id, ops, mode='mixed'):
        super().__init__()
        self.client_id = client_id
        self.ops = ops
        self.mode = mode
        self.latencies = []
        self.errors = 0

    def run(self):
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.connect((HOST, PORT))
            
            for i in range(self.ops):
                op_id = self.client_id * 1000000 + i
                if self.mode == 'write':
                    sql = INSERT_SQL.format(id=op_id, balance=random.uniform(0, 10000))
                elif self.mode == 'read':
                    sql = SELECT_SQL.format(id=random.randint(0, op_id if op_id > 0 else 1))
                else: # mixed
                    if random.random() < 0.2:
                        sql = INSERT_SQL.format(id=op_id, balance=random.uniform(0, 10000))
                    else:
                        sql = SELECT_SQL.format(id=random.randint(0, op_id if op_id > 0 else 1))
                
                start = time.time()
                sock.sendall(encode_frame(sql))
                res = decode_frame(sock)
                end = time.time()
                
                if not res or "ERR" in res:
                    self.errors += 1
                else:
                    self.latencies.append((end - start) * 1000)
            
            sock.close()
        except Exception as e:
            print(f"Client {self.client_id} error: {e}")
            self.errors += 1

def run_bench(mode='mixed', num_clients=NUM_CLIENTS, ops=OPS_PER_CLIENT):
    print(f"\n[[ Running Benchmark: {mode} mode, {num_clients} clients, {ops} ops/client ]]")
    
    # Initialize table
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((HOST, PORT))
        sock.sendall(encode_frame("CREATE TABLE IF NOT EXISTS BENCH_TABLE(ID DECIMAL PRIMARY KEY, NAME VARCHAR(64), BALANCE DECIMAL, EXPIRES_AT DECIMAL);"))
        decode_frame(sock)
        sock.sendall(encode_frame("DELETE FROM BENCH_TABLE;"))
        decode_frame(sock)
        sock.close()
    except Exception as e:
        print(f"Error during initialization: {e}")
        return

    clients = [Client(i, ops, mode) for i in range(num_clients)]
    start_time = time.time()
    for c in clients: c.start()
    for c in clients: c.join()
    end_time = time.time()
    
    duration = end_time - start_time
    total_ops = num_clients * ops
    throughput = total_ops / duration
    
    all_latencies = []
    total_errors = 0
    for c in clients:
        all_latencies.extend(c.latencies)
        total_errors += c.errors
        
    if all_latencies:
        avg_lat = sum(all_latencies) / len(all_latencies)
        p99_lat = sorted(all_latencies)[int(len(all_latencies) * 0.99)]
        print(f"Duration: {duration:.2f}s")
        print(f"Throughput: {throughput:.2f} ops/sec")
        print(f"Average Latency: {avg_lat:.2f} ms")
        print(f"P99 Latency: {p99_lat:.2f} ms")
        print(f"Errors: {total_errors}")
    else:
        print("Benchmark failed to collect latencies.")

if __name__ == "__main__":
    if len(sys.argv) > 1:
        NUM_CLIENTS = int(sys.argv[1])
    if len(sys.argv) > 2:
        OPS_PER_CLIENT = int(sys.argv[2])
        
    run_bench('write', NUM_CLIENTS, OPS_PER_CLIENT)
    run_bench('read', NUM_CLIENTS, OPS_PER_CLIENT)
    run_bench('mixed', NUM_CLIENTS, OPS_PER_CLIENT)
