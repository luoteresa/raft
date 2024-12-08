import grpc
import server_pb2_grpc
import server_pb2
import frontend
from concurrent import futures

class Client:
    def __init__(self, id):
        self.client_id = id
        self.channel = grpc.insecure_channel("localhost:8001")
        self.seq_num = 0
    
    def Get(self, key):
        stub = server_pb2.FrontEndStub("localhost:8001")
        self.seq_num += 1
        result = stub.Get(server_pb2.GetKey(key, self.client_id, self.seq_num))
        return result.value if not result.error else "ERROR"

    def Put(self, key, value):
        stub = server_pb2.FrontEndStub("localhost:8001")
        self.seq_num += 1
        result = stub.Put(server_pb2.KeyValue(key, value, self.client_id, 0))
        return result.value if not result.error else "ERROR"

    def Replace(self, key, value):
        stub = server_pb2.FrontEndStub("localhost:8001")
        self.seq_num += 1
        result = stub.Replace(server_pb2.KeyValue(key, value, self.client_id, 0))
        return result.value if not result.error else "ERROR"
    
if __name__ == '__main__':
    frontend_server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    server_pb2_grpc.add_FrontEndServicer_to_server(frontend.FrontEndHandler(), frontend_server)
    frontend_server.add_insecure_port("localhost:8001")
    frontend_server.start()

    client = Client()

    frontend_server.wait_for_termination()