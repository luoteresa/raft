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
        self.success_requests = 0

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
                    return result # Return the leader's channel
                
            except grpc.RpcError as e:
                print(f"Error contacting node at port {port}: {e}")
        
        # If no leader is found
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

        # frontend.py


    def StartRaft(self, request, context):
        num_nodes = request.arg
        processes = []

        for node_id in range(1, num_nodes + 1):
            # Create a new process for each server
            p = Process(target=server.start_server, args=(node_id, num_nodes), name=f"raftserver{node_id}")
            p.start()
            processes.append(p)
            self.peers[f"localhost:{9000 + node_id}"] = grpc.insecure_channel(f"localhost:{9000 + node_id}")

        print("All servers have been started as separate processes.")

        return server_pb2.Reply(wrongLeader=False, error="", value="")

    #def StartRaft(self, request, context):
        # num_nodes = request.arg
        # port = 9000
        # raft_port = 7000

        # # List to store all servers
        # servers = []
        # handlers = []

        # # Spin up all servers
        # for i in range(1, num_nodes + 1):
        #     # Create gRPC server

        #     raft_server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
        #     handler = server.ServerHandler(i, list(range(1, num_nodes + 1)), f"localhost:{port + i}")

        #     # Add servicers to the server
        #     server_pb2_grpc.add_KeyValueStoreServicer_to_server(handler, raft_server)
        #     server_pb2_grpc.add_RaftServicer_to_server(handler, raft_server)

        #     # Bind server to ports
        #     raft_server.add_insecure_port(f"localhost:{port + i}")
        #     raft_server.add_insecure_port(f"localhost:{raft_port + i}")

        #     # Add the server to the list
        #     servers.append(raft_server)
        #     handlers.append(handler)

        #     # Store peer channels
        #     self.peers[port + i] = grpc.insecure_channel(f"localhost:{port + i}")

        # # Start all servers
        # for cur_server in servers:
        #     cur_server.start()

        # print("All servers have started successfully.")
        
        # for cur_handler in handlers:
        #     cur_handler.start_raft()

        

        # # Wait for all servers to terminate
        # for cur_server in servers:
        #     cur_server.wait_for_termination()
        # num_nodes = request.arg 
        # port = 9000
        # raft_port = 7000
        # threads = []

        # for i in range(1, num_nodes + 1):
        #     def start_server(node_id):
        #         raft_server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
        #         handler = server.ServerHandler(node_id, list(range(1, num_nodes + 1)), f"localhost:{port + node_id}", True)

        #         server_pb2_grpc.add_KeyValueStoreServicer_to_server(handler, raft_server)
        #         server_pb2_grpc.add_RaftServicer_to_server(handler, raft_server)
                
        #         raft_server.add_insecure_port(f"localhost:{port + node_id}")
        #         raft_server.add_insecure_port(f"localhost:{raft_port + node_id}")

        #         # Start the server in a separate thread
        #         def serve():
        #             raft_server.start()
        #             print(f"Node {node_id} started on ports {port + node_id} and {raft_port + node_id}")
        #             threading.Thread(target=handler.run, daemon=True).start()  # Run RAFT protocol
        #             raft_server.wait_for_termination()

        #         thread = threading.Thread(target=serve, daemon=True)
        #         thread.start()
        #         threads.append(thread)

        #     start_server(i)

        # return server_pb2.Reply(wrongLeader=False, error="", value="")
