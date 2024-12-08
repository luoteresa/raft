import grpc
import server_pb2_grpc
import server_pb2
import server
from concurrent import futures

class FrontEndHandler(server_pb2_grpc.FrontEndServicer):

    def __init__(self):
        self.peers = {}
        self.leader_port = None
        self.success_requests = 0

    def broadcast_for_leader(self, request, operation):
        """Broadcast to all nodes to find the current leader."""
        for port, channel in self.peers.items():
            try:
                # Create stub for the current peer
                kv_stub = server_pb2.KeyValueStoreStub(channel)
                
                # Send GetState request to check if this node is the leader
                if operation == "Get":
                    result = kv_stub.Get(server_pb2.GetKey(request.key, request.client_id, request.request_id))
                elif operation == "Put":
                    result = kv_stub.Put(server_pb2.KeyValue(request.key, request.value, request.client_id, request.request_id))
                elif operation == "Replace":
                    result = kv_stub.Replace(server_pb2.KeyValue(request.key, request.value, request.client_id, request.request_id))

                if not result.wrongLeader:
                    print(f"Leader found at port {port}")
                    self.leader_port = port
                    return result # Return the leader's channel
                
            except grpc.RpcError as e:
                print(f"Error contacting node at port {port}: {e}")
        
        # If no leader is found
        print("No leader found.")
        return None

    def Get(self, request, context):
        channel = grpc.insecure_channel(self.leader_port)
        kv_stub = server_pb2_grpc.KeyValueStoreStub(channel)
        leader_get = kv_stub.Get(server_pb2.GetKey(request.key, request.client_id, request.request_id))
        
        while not leader_get or leader_get.wrongLeader:
            leader_get = self.broadcast_for_leader(request, "Get")

        reply = {"wrongLeader": leader_get.wrongLeader, "error": leader_get.error, "value": leader_get.value}

        return server_pb2.Reply(**reply)

    def Put(self, request, context):
        channel = grpc.insecure_channel(self.leader_port)
        kv_stub = server_pb2_grpc.KeyValueStoreStub(channel)
        leader_put = kv_stub.Get(server_pb2.GetKey(request.key, request.client_id, request.request_id))
        
        while not leader_put or leader_put.wrongLeader:
            leader_put = self.broadcast_for_leader(request, "Put")

        reply = {"wrongLeader": leader_put.wrongLeader, "error": leader_put.error, "value": leader_put.value}

        return server_pb2.Reply(**reply)

    def Replace(self, request, context):
        channel = grpc.insecure_channel(self.leader_port)
        kv_stub = server_pb2_grpc.KeyValueStoreStub(channel)
        leader_replace = kv_stub.Get(server_pb2.GetKey(request.key, request.client_id, request.request_id))
        
        while not leader_replace or leader_replace.wrongLeader:
            leader_replace = self.broadcast_for_leader(request, "Replace")

        reply = {"wrongLeader": leader_replace.wrongLeader, "error": leader_replace.error, "value": leader_replace.value}

        return server_pb2.Reply(**reply)

    def StartRaft(self, request, context):
        num_nodes = request.arg 
        port = 9000
        raft_port = 7000
        for i in range(1, num_nodes + 1):
            raft_server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
            handler = server.ServerHandler(i, list(range(1, num_nodes + 1)), f"localhost:{port + i}")

            server_pb2_grpc.add_KeyValueStoreServicer_to_server(handler, raft_server)
            server_pb2_grpc.add_RaftServicer_to_server(handler, raft_server)
            
            raft_server.add_insecure_port(f"localhost:{port + i}")
            raft_server.add_insecure_port(f"localhost:{raft_port + i}")
            self.peers[port + i] = grpc.insecure_channel(f"localhost:{port + i}")

            raft_server.start()
            raft_server.wait_for_termination()