import grpc
import server_pb2_grpc
import server_pb2
import frontend
from concurrent import futures
import time

CLIENT_ID = 0

class Client:
    def __init__(self):
        global CLIENT_ID
        CLIENT_ID += 1
        self.client_id = CLIENT_ID
        self.frontend_port = "localhost:8001"
        self.seq_num = 0
    
    def Get(self, key):
        channel = grpc.insecure_channel(self.frontend_port)
        stub = server_pb2_grpc.FrontEndStub(channel)
        self.seq_num += 1
        result = stub.Get(server_pb2.GetKey(key=key, ClientId=self.client_id, RequestId=self.seq_num))
        return result.value if not result.error else "ERROR"

    def Put(self, key, value):
        channel = grpc.insecure_channel(self.frontend_port)
        stub = server_pb2_grpc.FrontEndStub(channel)
        self.seq_num += 1
        result = stub.Put(server_pb2.KeyValue(key=key, value=value, ClientId=self.client_id, RequestId=self.seq_num))
        return result.value if not result.error else "ERROR"

    def Replace(self, key, value):
        channel = grpc.insecure_channel(self.frontend_port)
        stub = server_pb2_grpc.FrontEndStub(channel)
        self.seq_num += 1
        result = stub.Replace(server_pb2.KeyValue(key=key, value=value, ClientId=self.client_id, RequestId=0))
        return result.value if not result.error else "ERROR"
    
    def StartRaft(self, num_nodes):
        channel = grpc.insecure_channel(self.frontend_port)
        stub = server_pb2_grpc.FrontEndStub(channel)
        self.seq_num += 1
        result = stub.StartRaft(server_pb2.IntegerArg(arg=num_nodes))
        return result.value if not result.error else "ERROR"
    
if __name__ == '__main__':
    frontend_server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    server_pb2_grpc.add_FrontEndServicer_to_server(frontend.FrontEndHandler(), frontend_server)
    frontend_server.add_insecure_port("localhost:8001")
    frontend_server.start()

    channel = grpc.insecure_channel("localhost:8001")
    stub = server_pb2_grpc.FrontEndStub(channel)
    stub.StartRaft(server_pb2.IntegerArg(arg=5))
    time.sleep(5)
    result = stub.Put(server_pb2.KeyValue(key="key1", value="10", ClientId=1, RequestId=1)) # returns "10"
    print("FINISHED PUT KEY1 10")
    print("PUT KEY1 10 RESULT SHOULD BE 10", result)
    result = stub.Get(server_pb2.GetKey(key="key1", ClientId=1, RequestId=2)) # returns "10"
    print("FINISHED GET KEY1")
    print("GET KEY1 RESULT SHOULD BE 10", result)
    result = stub.Replace(server_pb2.KeyValue(key="key1", value="20", ClientId=1, RequestId=3)) # returns "20"
    print("FINISHED REPLACE KEY1 20")
    print("REPLACE KEY1 20 RESULT SHOULD BE 20", result)
    result = stub.Get(server_pb2.GetKey(key="key1", ClientId=1, RequestId=4)) # returns "20"
    print("FINISHED GET KEY1")
    print("GET KEY1 RESULT SHOULD BE 20", result)
    result = stub.Get(server_pb2.GetKey(key="key2", ClientId=1, RequestId=5)) # returns error
    print("FINISHED GET KEY2")
    print("GET KEY2 RESULT SHOULD BE ERROR", result)
    result = stub.Put(server_pb2.KeyValue(key="key2", value="30", ClientId=1, RequestId=6)) # add key2, returns "30"
    print("FINISHED PUT KEY2 30")
    print("PUT KEY2 30 RESULT SHOULD BE 30", result)
    result = stub.Get(server_pb2.GetKey(key="key2", ClientId=1, RequestId=5)) # returns "30"
    print("FINISHED GET KEY2")
    print("GET KEY2 RESULT SHOULD BE 30", result)
    result = stub.Replace(server_pb2.KeyValue(key="key3", value="40", ClientId=1, RequestId=7)) # returns error
    print("FINISHED REPLACE KEY3 40")
    print("REPLACE KEY3 40 RESULT SHOULD BE ERROR", result)

    while True:
        frontend_server.wait_for_termination()