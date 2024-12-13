import grpc # type: ignore
import server_pb2 as pb2
import server_pb2_grpc as pb2_grpc
import random
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from setproctitle import setproctitle
from persist_state import PersistentStateStore

ELECTION_TIMEOUT_MIN = 1
ELECTION_TIMEOUT_MAX = 2
HEARTBEAT_INTERVAL = 0.3

class LogEntry:
    def __init__(self, key, value, term):
        self.key = key
        self.value = value
        self.term = term

class ServerHandler(pb2_grpc.RaftServicer, pb2_grpc.KeyValueStoreServicer):
    def __init__(self, node_id, membership_ids, port, verbose=False):
        self.node_id = node_id # Unique ID for self
        self.membership_ids = membership_ids # List of node IDs
        self.port = port # Port number to listen on
        self.current_term = 0 # Current term of self
        self.voted_for = None # ID of candidate voted for
        self.log = [] # Persistent log of key-value operations
        self.commit_length = 0 # Index of last committed log entry
        self.current_role = 'Follower' # Follower, Candidate, or Leader
        self.current_leader = None # ID of current leader
        self.votes_received = set() # IDs of servers that voted for self
        self.sent_length = {} # Maps follower IDs to length of logs sent
        self.acked_length = {} # Maps follower IDs to length of logs acked
        self.raft_peers = {} # Map membership IDs to gRPC connections
        self.kv_store = {} # Key value store
        self.persist_store = PersistentStateStore(f"log_node{self.node_id}.pkl", f"index_node{self.node_id}.log")
        self.election_timer = None
        self.heartbeat_timer = None
        # self.ack_condition = threading.Condition()  # Condition variable for acks needed to commit
        self.initialize_sent_acked_length()
        self.initialize_connections()
        
        self.verbose = verbose

    def initialize_sent_acked_length(self):
        for node_id in self.membership_ids:
            self.sent_length[node_id] = 0
            self.acked_length[node_id] = 0

    def initialize_connections(self):
        start_port = 7001 + (self.node_id - 1) * len(self.membership_ids)
        for member_id in self.membership_ids:
            if self.node_id != member_id:
                channel = grpc.insecure_channel(f"localhost:{start_port + (member_id - 1)}")
                raft_stub = pb2_grpc.RaftStub(channel)
                self.raft_peers[member_id] = raft_stub
    
    def reset_timer(self, timer, timeout, function, *args):
        if timer:
            timer.cancel()
        timer = threading.Timer(timeout, function, args)
        timer.start()
        return timer

    # Invoked by: Follower (who suspects Leader fail), Candidate
    def start_election(self):
        if self.current_role == 'Leader':
            return
        
        if self.verbose:
            print(f"STARTING ELECTION ON NODE {self.node_id}")
        
        self.current_term += 1
        self.current_role = 'Candidate'
        self.voted_for = self.node_id
        self.votes_received = {self.node_id}
        log_term = self.log[-1].term if self.log else 0

        def request_vote(node_id):
            request = pb2.VoteRequest(
                term=self.current_term,
                candidateId=self.node_id,
                lastLogIndex=len(self.log),
                lastLogTerm=log_term
            )
            try:
                response = self.raft_peers[node_id].RequestVote(request)
                self.on_vote_response(node_id, response.term, response.voteGranted)
            except grpc.RpcError as e:
                # print(f"Error contacting node {node_id}: {e}")
                pass

        with ThreadPoolExecutor() as executor:
            for node_id in self.membership_ids:
                if node_id != self.node_id:
                    executor.submit(request_vote, node_id)

        self.election_timer = self.reset_timer(
            self.election_timer, random.uniform(ELECTION_TIMEOUT_MIN, ELECTION_TIMEOUT_MAX), self.start_election
        ) 

    # Invoked by: Follower, Candidate, Leader
    # RequestVote RPC
    def on_vote_request(self, candidate_id, candidate_term, candidate_log_length, candidate_log_term):
        if candidate_term > self.current_term:
            self.current_term = candidate_term
            self.current_role = 'Follower'
            self.voted_for = None
        
        log_term = self.log[-1].term if len(self.log) else 0
        candidate_log_better = (candidate_log_term > log_term) or \
            (candidate_log_term == log_term and candidate_log_length > len(self.log))
        
        if candidate_term == self.current_term and candidate_log_better and self.voted_for in [candidate_id, None]:
            self.voted_for = candidate_id
            return True
        else:
            return False
        
    # Invoked by: Candidate
    def on_vote_response(self, voter_id, term, granted):
        if term > self.current_term:
            self.current_term = term
            self.current_role = 'Follower'
            self.voted_for = None
            if self.election_timer:
                self.election_timer.cancel()
                self.election_timer = None
        elif self.current_role != 'Leader' and self.current_term == term and granted:
            self.votes_received.add(voter_id)
            if self.verbose:
                print(f"NODE {self.node_id} RECEIVED VOTE FROM NODE ", voter_id)
                print(f"NODE {self.node_id} HAS VOTES FROM {self.votes_received}")
            if len(self.votes_received) >= len(self.membership_ids) // 2 + 1:
                print(f"Node {self.node_id} got the majority and is now leader")
                self.current_role = 'Leader'
                self.current_leader = self.node_id
                
                if self.election_timer:
                    self.election_timer.cancel()
                    self.election_timer = None
                    
                # Start sending heartbeats
                self.leader_heartbeat()  

                def replicate_to_follower(follower_id):
                    self.sent_length[follower_id] = len(self.log)
                    self.acked_length[follower_id] = 0
                    self.replicate_log(self.node_id, follower_id)

                with ThreadPoolExecutor() as executor:
                    for follower_id in self.membership_ids:
                        if follower_id != self.node_id:
                            executor.submit(replicate_to_follower, follower_id) 

    # Invoked by: Follower
    def append_entries(self, prefix_length, leader_commit, entries):
        # Append entries to the log and replicate
        if len(entries) > 0 and len(self.log) > prefix_length:
            index = min(len(self.log), prefix_length + len(entries)) - 1
            if self.log[index].term != entries[index - prefix_length].term:
                self.log = self.log[:prefix_length]
        if prefix_length + len(entries) > len(self.log):
            for i in range(len(self.log) - prefix_length, len(entries)):
                self.log.append(entries[i])
        if leader_commit > self.commit_length:
            for i in range(self.commit_length, leader_commit):
                # TODO: Deliver log[i].msg to the application -- Done in RPC implementation?
                log_entry = self.log[i]
                self.kv_store[log_entry.key] = log_entry.value
                self.persist_store.save_state(log_entry)
            self.commit_length = leader_commit
    
    def get_acks_for_length(self, length):
        acks = 0
        for node_id in self.membership_ids:
            if self.acked_length[node_id] >= length:
                acks += 1
        return acks
    
    # Invoked by: Leader  
    def commit_log_entries(self):
        minimum_acks = len(self.membership_ids) // 2 + 1
        highest_length = -1
        for length in range(self.commit_length, len(self.log) + 1):
            if self.get_acks_for_length(length) >= minimum_acks and length > highest_length:
                highest_length = length
        if highest_length != -1 and highest_length > self.commit_length and self.log[highest_length - 1].term == self.current_term:
            for i in range(self.commit_length - 1, highest_length):
                # TODO: Deliver log[i].msg to the application -- Done in the RPC implementation
                log_entry = self.log[i]
                self.kv_store[log_entry.key] = log_entry.value
            self.commit_length = highest_length
    
    # Invoked by: Leader
    def replicate_log(self, leader_id, follower_id):
        prefix_length = self.sent_length[follower_id]
        suffix = self.log[prefix_length:]
        prefix_term = self.log[prefix_length - 1].term if prefix_length > 0 else 0
        request = pb2.AppendEntriesRequest(
            leaderId=leader_id,
            term=self.current_term,
            prefixLength=prefix_length,
            prefixTerm=prefix_term,
            leaderCommit=self.commit_length,
            entries=[pb2.LogEntry(term=entry.term, key=entry.key, value=entry.value) for entry in suffix]
        )
        
        try:
            response = self.raft_peers[follower_id].AppendEntries(request)
            self.on_log_response(follower_id, response.term, response.newAckedLength, response.success)
        except grpc.RpcError as e:
            # print(f"Error replicating log to node {follower_id}: {e}")
            pass
    
    # Invoked by: Follower
    # Equivalent to AppendEntries RPC in paper, is a wrapper that also checks for term
    def on_log_request(self, leader_id, term, prefix_length, prefix_term, leader_commit, entries):
        if term > self.current_term:
            self.current_term = term
            self.voted_for = None
            
            if self.election_timer:
                self.election_timer.cancel()
                self.election_timer = None
            
        if term == self.current_term:
            self.current_role = 'Follower' 
            self.current_leader = leader_id
            
        # Received heartbeat from leader, reset election timer
        self.election_timer = self.reset_timer(
            self.election_timer, random.uniform(ELECTION_TIMEOUT_MIN, ELECTION_TIMEOUT_MAX), self.start_election
        )

        log_ok = (len(self.log) >= prefix_length) and \
            (prefix_length == 0 or self.log[prefix_length - 1].term == prefix_term)
        if log_ok:
            self.append_entries(prefix_length, leader_commit, entries)
            new_acked_length = prefix_length + len(entries)
            return new_acked_length, True
        else:
            return 0, False
            
    # Invoked by: Leader
    def on_log_response(self, follower_id, term, new_acked_length, success):
        if term == self.current_term and self.current_role == 'Leader':
            if success and new_acked_length >= self.acked_length[follower_id]:
                self.sent_length[follower_id] = new_acked_length
                self.acked_length[follower_id] = new_acked_length
                # Notify that we have another ack
                # with self.ack_condition:
                #     self.ack_condition.notify_all()
                self.commit_log_entries()
            elif self.sent_length[follower_id] > 0:
                self.sent_length[follower_id] -= 1
                self.replicate_log(self.node_id, follower_id)
            elif term > self.current_term:
                self.current_term = term
                self.current_role = 'Follower'
                self.voted_for = None
                
                if self.heartbeat_timer:
                    self.heartbeat_timer.cancel()
                    self.heartbeat_timer = None
                    
                if self.election_timer:
                    self.election_timer.cancel()
                    self.election_timer = None
                    
                # self.election_timer = self.reset_timer(
                #     self.election_timer, random.uniform(ELECTION_TIMEOUT_MIN, ELECTION_TIMEOUT_MAX), self.start_election
                # )
                
                # self.election_timer = self.reset_timer(
                #     self.election_timer, 
                #     random.uniform(ELECTION_TIMEOUT_MIN, ELECTION_TIMEOUT_MAX), 
                #     self.start_election
                # )
                
    # Invoked by: All            
    def crash_recovery(self):
        self.current_role = 'Follower'
        self.current_leader = None
        self.votes_received = set()
        self.sent_length = {}
        self.acked_length = {}
        self.log = self.persist_store.load_all_states()
        for entry in self.log:
            self.kv_store[entry.key] = self.kv_store[entry.value]
        self.commit_length = len(self.log)
        self.current_term = self.log[-1].term
    
    # Invoked by: Leader
    def leader_heartbeat(self):
        def send_heartbeat(follower_id):
            self.replicate_log(self.node_id, follower_id)

        with ThreadPoolExecutor() as executor:
            for follower_id in self.membership_ids:
                if follower_id != self.node_id:
                    executor.submit(send_heartbeat, follower_id)

        self.heartbeat_timer = self.reset_timer(
            self.heartbeat_timer, HEARTBEAT_INTERVAL, self.leader_heartbeat
        )
            
    ### GRPC IMPLEMENTATIONS ###
    
    def GetState(self, request, context):
        return pb2.State(term=self.current_term, isLeader=(self.current_leader == self.node_id))

    def Get(self, request, context):
        if self.current_role != 'Leader':
            return pb2.Reply(
                wrongLeader=True,
                error=f"Node ID {self.node_id} is not the leader.",
                value=""
            )

        if request.key in self.kv_store:
            return pb2.Reply(
                wrongLeader=False,
                error="",
                value=self.kv_store[request.key]
            )
        else:
            return pb2.Reply(
                wrongLeader=False,
                error=f"Key {request.key} not found.",
                value=""
            )

    def Put(self, request, context):
        if self.current_role != 'Leader':
            return pb2.Reply(
                wrongLeader=True,
                error=f"Node ID {self.node_id} is not the leader.",
                value=""
            )

        log_entry = LogEntry(request.key, request.value, self.current_term)
        self.log.append(log_entry)
        self.acked_length[self.node_id] = len(self.log)
        
        if self.verbose:
            print(f"GRPC Put {request.key}, {request.value} at node {self.node_id}") 

        with ThreadPoolExecutor() as executor:
            for follower_id in self.membership_ids:
                if follower_id != self.node_id:
                    executor.submit(self.replicate_log, self.node_id, follower_id)  
                    
        # Wait for majority acknowledgment using a condition, not busy waiting
        # minimum_acks = len(self.membership_ids) // 2 + 1

        # with self.ack_condition:
        #     while self.get_acks_for_length(len(self.log)) < minimum_acks:
        #         self.ack_condition.wait()  

        # Wait for majority acknowledgment
        minimum_acks = len(self.membership_ids) // 2 + 1
        while self.get_acks_for_length(len(self.log)) < minimum_acks:
            time.sleep(0.01)

        if self.verbose:
            print("acks", self.get_acks_for_length(len(self.log)))

        self.commit_log_entries()

        if request.key in self.kv_store and self.kv_store[request.key] == request.value:
            return pb2.Reply(
                wrongLeader=False,
                error="",
                value=request.value
            )
        else:
            return pb2.Reply(
                wrongLeader=False,
                error="Failed to commit the log entry.",
                value=""
            )

    def Replace(self, request, context): 
        if self.current_role != 'Leader':
            return pb2.Reply(
                wrongLeader=True,
                error=f"Node ID {self.node_id} is not the leader.",
                value=""
            )

        if request.key not in self.kv_store:
            return pb2.Reply(
                wrongLeader=False,
                error=f"Key {request.key} does not exist.",
                value=""
            )

        log_entry = LogEntry(request.key, request.value, self.current_term)
        self.log.append(log_entry)
        self.acked_length[self.node_id] = len(self.log)

        with ThreadPoolExecutor() as executor:
            for follower_id in self.membership_ids:
                if follower_id != self.node_id:
                    executor.submit(self.replicate_log, self.node_id, follower_id)

        # Wait for majority acknowledgment
        minimum_acks = len(self.membership_ids) // 2 + 1
        while self.get_acks_for_length(len(self.log)) < minimum_acks:
            time.sleep(0.01)

        self.commit_log_entries()

        if request.key in self.kv_store and self.kv_store[request.key] == request.value:
            return pb2.Reply(
                wrongLeader=False,
                error="",
                value=request.value
            )
        else:
            return pb2.Reply(
                wrongLeader=False,
                error="Failed to commit the log entry.",
                value=""
            )

    def AppendEntries(self, request, context):
        new_acked_length, success = self.on_log_request(
            request.leaderId, request.term, request.prefixLength,
            request.prefixTerm, request.leaderCommit, request.entries
        )
        return pb2.AppendEntriesReply(
            term=self.current_term,
            newAckedLength=new_acked_length,
            success=success
        )

    def RequestVote(self, request, context):
        vote_granted = self.on_vote_request(
            request.term, request.candidateId, request.lastLogIndex + 1, request.lastLogTerm
        )
        return pb2.VoteReply(
            term=self.current_term,
            voteGranted=vote_granted
        )
    
    ### RUNNERS ###
            
    def start_raft(self):
        self.election_timer = self.reset_timer(
            self.election_timer, random.uniform(ELECTION_TIMEOUT_MIN, ELECTION_TIMEOUT_MAX), self.start_election
        )
    
def start_server(node_id, num_nodes):
    """Starts a single gRPC server for the given node_id."""
    port = 9001 + (node_id - 1)
    setproctitle(f"raftserver{node_id}")

    # Initialize the gRPC server
    raft_server = grpc.server(ThreadPoolExecutor(max_workers=5))
    handler = ServerHandler(
        node_id, 
        list(range(1, num_nodes + 1)), 
        f"localhost:{port}",
        True
    )

    # Register services
    pb2_grpc.add_KeyValueStoreServicer_to_server(handler, raft_server)
    pb2_grpc.add_RaftServicer_to_server(handler, raft_server)

    # Bind to ports
    raft_server.add_insecure_port(f"localhost:{port}")
    
    for member_id in range(1, num_nodes + 1):
        other_start_port = 7001 + (member_id - 1) * num_nodes
        if node_id != member_id:
            raft_server.add_insecure_port(f"localhost:{other_start_port + (node_id - 1) }")
            print(f"Connecting to port {other_start_port + (node_id - 1)}")

    # Start RAFT protocol for leader election
    handler.start_raft()

    # Start the gRPC server
    raft_server.start()

    # Wait for termination
    raft_server.wait_for_termination()