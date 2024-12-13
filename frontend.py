import time
import grpc
import server_pb2_grpc
import server_pb2
import server
from concurrent import futures
from multiprocessing import Process

class FrontEndHandler(server_pb2_grpc.FrontEndServicer):

    def __init__(self):
        self.peers = {}
        self.current_leader = None
        self.current_leader_stub = None

    def find_leader(self, max_retries=5, delay=1):
        for _ in range(max_retries):
            for addr, stub in self.peers.items():
                try:
                    state = stub.GetState(server_pb2.Empty())
                    if state.isLeader:
                        self.current_leader = addr
                        self.current_leader_stub = stub
                        return
                except grpc.RpcError:
                    pass
            time.sleep(delay)

        self.current_leader = None
        self.current_leader_stub = None

    def forward_request(self, rpc_func, request):
        if self.current_leader_stub is None:
            self.find_leader()
            if self.current_leader_stub is None:
                return server_pb2.Reply(wrongLeader=True, error="No leader found", value="")

        try:
            response = rpc_func(request)
            if response.wrongLeader:
                self.find_leader()
                if self.current_leader_stub is None:
                    return server_pb2.Reply(wrongLeader=True, error="No leader found", value="")
                return rpc_func(request)
            return response

        except grpc.RpcError as e:
            self.find_leader()
            if self.current_leader_stub is None:
                return server_pb2.Reply(wrongLeader=True, error="No leader found after retry", value="")
            try:
                response = rpc_func(request)
                return response
            except grpc.RpcError:
                return server_pb2.Reply(wrongLeader=True, error="Failed after retry", value="")

    def Get(self, request, context):
        return self.forward_request(lambda req: self.current_leader_stub.Get(req), request)

    def Put(self, request, context):
        return self.forward_request(lambda req: self.current_leader_stub.Put(req), request)

    def Replace(self, request, context):
        return self.forward_request(lambda req: self.current_leader_stub.Replace(req), request)

    def StartRaft(self, request, context):
        num_nodes = request.arg
        processes = []

        for node_id in range(1, num_nodes + 1):
            p = Process(target=server.start_server, args=(node_id, num_nodes), name=f"raftserver{node_id}", daemon=True)
            p.start()
            processes.append(p)
            channel = grpc.insecure_channel(f"localhost:{9000 + node_id}")
            self.peers[node_id] = server_pb2_grpc.KeyValueStoreStub(channel)

        return server_pb2.Reply(wrongLeader=False, error="", value="")

if __name__ == '__main__':
    frontend_server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    server_pb2_grpc.add_FrontEndServicer_to_server(FrontEndHandler(), frontend_server)
    frontend_server.add_insecure_port("localhost:8001")
    frontend_server.start()
    frontend_server.wait_for_termination()
