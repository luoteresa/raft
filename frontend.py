import grpc # type: ignore
import server_pb2_grpc
import server_pb2
import server
import threading
from concurrent import futures
from multiprocessing import Process

class FrontEndHandler(server_pb2_grpc.FrontEndServicer):

    def __init__(self):
        self.peers = {}
        self.leader_port = "localhost:9001"
        self.request_id = 0

    def broadcast_for_leader(self, request, operation):
        """Broadcast to all nodes to find the current leader."""
        for port, channel in self.peers.items():
            try:
                # Create stub for the current peer
                kv_stub = server_pb2_grpc.KeyValueStoreStub(channel)
                
                # Send GetState request to check if this node is the leader
                if operation == "Get":
                    result = kv_stub.Get(server_pb2.GetKey(key=request.key, ClientId=request.ClientId, RequestId=request.RequestId))
                elif operation == "Put":
                    result = kv_stub.Put(server_pb2.KeyValue(key=request.key, value=request.value, ClientId=request.ClientId, RequestId=request.RequestId))
                elif operation == "Replace":
                    result = kv_stub.Replace(server_pb2.KeyValue(key=request.key, value=request.value, ClientId=request.ClientId, RequestId=request.RequestId))

                if not result.wrongLeader:
                    print(f"Leader found at port {port}")
                    self.leader_port = port
                    return result
                
            except grpc.RpcError as e:
                print(f"Error contacting node at port {port}: {e}")
        
        print("No leader found.")
        return None

    def Get(self, request, context):
        channel = grpc.insecure_channel(self.leader_port)
        kv_stub = server_pb2_grpc.KeyValueStoreStub(channel)
        leader_get = kv_stub.Get(server_pb2.GetKey(key=request.key, ClientId=request.ClientId, RequestId=request.RequestId))
        
        while not leader_get or leader_get.wrongLeader:
            leader_get = self.broadcast_for_leader(request, "Get")

        reply = {"wrongLeader": leader_get.wrongLeader, "error": leader_get.error, "value": leader_get.value}

        return server_pb2.Reply(**reply)

    def Put(self, request, context):
        channel = grpc.insecure_channel(self.leader_port)
        kv_stub = server_pb2_grpc.KeyValueStoreStub(channel)
        leader_put = kv_stub.Put(server_pb2.KeyValue(key=request.key, value=request.value, ClientId=request.ClientId, RequestId=request.RequestId))
        
        while not leader_put or leader_put.wrongLeader:
            leader_put = self.broadcast_for_leader(request, "Put")

        reply = {"wrongLeader": leader_put.wrongLeader, "error": leader_put.error, "value": leader_put.value}

        return server_pb2.Reply(**reply)

    def Replace(self, request, context):
        channel = grpc.insecure_channel(self.leader_port)
        kv_stub = server_pb2_grpc.KeyValueStoreStub(channel)
        leader_replace = kv_stub.Replace(server_pb2.KeyValue(key=request.key, value=request.value, ClientId=request.ClientId, RequestId=request.RequestId))
        
        while not leader_replace or leader_replace.wrongLeader:
            leader_replace = self.broadcast_for_leader(request, "Replace")

        reply = {"wrongLeader": leader_replace.wrongLeader, "error": leader_replace.error, "value": leader_replace.value}

        return server_pb2.Reply(**reply)

    def StartRaft(self, request, context):
        num_nodes = request.arg
        processes = []

        for node_id in range(1, num_nodes + 1):
            # Create a new process for each server
            p = Process(target=server.start_server, args=(node_id, num_nodes), name=f"raftserver{node_id}", daemon=True)
            p.start()
            processes.append(p)
            self.peers[f"localhost:{9000 + node_id}"] = grpc.insecure_channel(f"localhost:{9000 + node_id}")

        return server_pb2.Reply(wrongLeader=False, error="", value="")
