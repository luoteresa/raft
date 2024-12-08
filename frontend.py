import grpc
import frontend_pb2
import frontend_pb2_grpc
from concurrent import futures

class FrontEnd:

    def __init__(self):
        self.peers = {}
        self.leader_id = None
        self.clients = {}

    

    

