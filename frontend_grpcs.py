import frontend_pb2_grpc
import frontend_pb2
import keyvaluestore_pb2
import keyvaluestore_pb2_grpc
import grpc
import server
from concurrent import futures

peers = {}

class FrontEndHandler(frontend_pb2_grpc.FrontEndServicer):

    def broadcast_for_leader(self):
        """Broadcast to all nodes to find the current leader."""
        for port, channel in peers.items():
            try:
                # Create stub for the current peer
                kv_stub = keyvaluestore_pb2_grpc.KeyValueStoreStub(channel)
                
                # Send GetState request to check if this node is the leader
                state = kv_stub.GetState(**{})
                if state.isLeader:
                    print(f"Leader found at port {port}")
                    return channel  # Return the leader's channel
            except grpc.RpcError as e:
                print(f"Error contacting node at port {port}: {e}")
        
        # If no leader is found
        print("No leader found.")
        return None

    def Get(self, request, context):
        channel = grpc.insecure_channel('localhost:7000')
        kv_stub = keyvaluestore_pb2_grpc.KeyValueStoreStub(channel)
        leader_state = kv_stub.GetState(**{})
        
        if not leader_state.isLeader:
            new_leader_channel = self.broadcast_for_leader()
            if new_leader_channel is not None:
                kv_stub = keyvaluestore_pb2_grpc.KeyValueStoreStub(new_leader_channel)
        
        result = kv_stub.Get(request.key)
        # response = kv_stub.Get(request.key)
        # while response wrongLeader is True;
        #     find new leader
        #     retry request

        # implement error checking for when key does not exist
        reply = {"wrongLeader": False, "error": False, "value": result.value}

        return frontend_pb2.Reply(**reply)

    def Put(self, request, context):
        pass

    def Replace(self, request, context):
        pass

    def StartRaft(self, request, context):
        num_nodes = request.arg 
        port = 9000
        for i in range(1, num_nodes + 1):
            raft_server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
            keyvaluestore_pb2_grpc.add_KeyValueStoreServicer_to_server(server.ServerHandler(), raft_server)
            raft_server.add_insecure_port(f"localhost:{port + i}")
            peers[port + i] = grpc.insecure_channel(f"localhost:{port + i}")
            raft_server.start()
            raft_server.wait_for_termination()