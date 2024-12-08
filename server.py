import grpc
import server_pb2 as pb2
import server_pb2_grpc as pb2_grpc
import random
import time

class LogEntry:
    def __init__(self, key, value, term):
        self.key = key
        self.value = value
        self.term = term

class ServerHandler(pb2_grpc.RaftServicer, pb2_grpc.KeyValueStoreServicer):
    def __init__(self, node_id, membership_ids, port):
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
        self.initialize_connections()

    def initialize_connections(self):
        for member_id in self.membership_ids:
            channel = grpc.insecure_channel(f"localhost:{7000 + member_id}")
            raft_stub = pb2.RaftStub(channel)
            self.raft_peers[member_id] = raft_stub

    # Invoked by: Follower (who suspects Leader fail), Candidate
    def start_election(self):
        self.current_term += 1
        self.current_role = 'Candidate'
        self.voted_for = self.node_id
        self.votes_received = {self.node_id}
        log_term = self.log[-1].term if len(self.log) else 0 
        
        # Broadcast VoteRequest to all members
        for node_id in self.membership_ids:
            if node_id != self.node_id:
                request = pb2.VoteRequest(
                    term=self.current_term,
                    candidateId=self.node_id,
                    lastLogIndex=len(self.log),
                    lastLogTerm=log_term
                )
                try:
                    # TODO: Thread so not blocked waiting for every node to respond
                    response = self.raft_peers[node_id].RequestVote(request)
                    self.on_vote_response(node_id, response.term, response.voteGranted)
                except grpc.RpcError as e:
                    print(f"Error contacting node {node_id}: {e}")

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
        
        if candidate_term == self.term and candidate_log_better and self.voted_for in [candidate_id, None]:
            self.voted_for = candidate_id
            return True
        else:
            return False
        
    # Invoked by: Candidate
    def on_vote_response(self, voter_id, term, granted):
        if self.current_term == term and granted:
            self.votes_received.add(voter_id)
            if len(self.votes_received) >= len(self.membership_ids) // 2 + 1:
                self.current_role = 'Leader'
                self.current_leader = self.node_id
                # TODO: Cancel election timer
                for follower_id in self.membership_ids:
                    if follower_id == self.node_id:
                        continue
                    self.sent_length[follower_id] = len(self.log)
                    self.acked_length[follower_id] = 0
                    self.replicate_log(self.node_id, follower_id)
        elif term > self.current_term:
            self.current_term = term
            self.current_role = 'Follower'
            self.voted_for = None
            # TODO: Cancel election timer
            
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
                # TODO: Deliver log[i].msg to the application
                pass
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
            for i in range(self.commit_length, highest_length - 1):
                # TODO: Deliver log[i].msg to the application
                pass
            self.commit_length = highest_length
    
    # Invoked by: Leader
    # LogRequest w/ empty suffix serves as heartbeat to let Followers know Leader still alive
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
        
        # TODO: Thread so not blocked waiting for node to respond
        try:
            response = self.raft_peers[follower_id].AppendEntries(request)
            self.on_log_response(follower_id, response.term, response.newAckedLength, response.Success)
        except grpc.RpcError as e:
            print(f"Error replicating log to node {follower_id}: {e}")
    
    # Invoked by: Follower
    # Equivalent to AppendEntries RPC in paper, is a wrapper that also checks for term
    def on_log_request(self, leader_id, term, prefix_length, prefix_term, leader_commit, entries):
        if term > self.current_term:
            self.current_term = term
            self.voted_for = None
            # TODO: Cancel election timer
        elif term == self.current_term:
            self.current_role = 'Follower' 
            self.current_leader = leader_id
            log_ok = (len(self.log) >= prefix_length) and \
                (prefix_length == 0 or self.log[-1].term == prefix_term)
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
                self.commit_log_entries()
            elif self.sent_length[follower_id] > 0:
                self.sent_length[follower_id] -= 1
                self.replicate_log(self.node_id, follower_id)
            elif term > self.current_term:
                self.current_term = term
                self.current_role = 'Follower'
                self.voted_for = None
                # TODO: Cancel election timer
                
    # Invoked by: All            
    def crash_recovery(self):
        self.current_role = 'Follower'
        self.current_leader = None
        self.votes_received = set()
        self.sent_length = {}
        self.acked_length = {}
    
    # Invoked by: Leader
    def leader_heartbeat(self):
        if self.current_role == 'Leader':
            for follower_id in self.membership_ids:
                if follower_id != self.node_id:
                    self.replicate_log(self.node_id, follower_id)
        
    ### GRPC IMPLEMENTATIONS ###
    
    def GetState(self, request, context):
        reply = {"term": self.current_term, "isLeader": self.current_leader == self.node_id}
        return pb2.State(**reply)

    def Get(self, request, context):
        if self.current_role != 'Leader':
            return pb2.Reply(
                wrongLeader=True,
                error=f"Node ID \{self.node_id} is not the leader.",
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
                error=f"Key \{request.key} not found.",
                value=""
            )

    def Put(self, request, context):
        if self.current_role != 'Leader':
            return pb2.Reply(
                wrongLeader=True,
                error=f"Node ID {self.node_id} is not the leader.",
                value=""
            )
            
         # Append to the Leader log
        log_entry = LogEntry(request.key, request.value, self.current_term)
        self.log.append(log_entry)
        self.acked_length[self.node_id] = len(self.log)
    
        # Replicate log to followers
        for follower_id in self.membership_ids:
            if follower_id != self.node_id:
                self.replicate_log(self.node_id, follower_id)
        
        # Wait for commitment
        while len(self.log) > self.commit_length:
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
            
        # Validate the operation
        if request.key not in self.kv_store:
            return pb2.Reply(
                wrongLeader=False,
                error=f"Key {request.key} does not exist.",
                value=""
            )
            
        # Append to the Leader log
        log_entry = LogEntry(request.key, request.value, self.current_term)
        self.log.append(log_entry)
        self.acked_length[self.node_id] = len(self.log)
    
        # Replicate log to followers
        for follower_id in self.membership_ids:
            if follower_id != self.node_id:
                self.replicate_log(self.node_id, follower_id)
        
        # Wait for commitment
        while len(self.log) > self.commit_length:
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
        new_acked_length, success = self.on_log_request(request.leaderId, request.term, request.prefixLength, request.prefixTerm, request.leaderCommit, request.entries)
        reply = {"followerId": self.node_id, "term": self.current_term, "newAckedLength": new_acked_length, "success": success}
        return pb2.AppendEntriesReply(**reply)

    def RequestVote(self, request, context):
        vote_granted = self.on_vote_request(request.term, request.candidateId, request.lastLogIndex + 1, request.lastLogTerm)
        reply = {"term": self.current_term, "voteGranted": vote_granted}
        return pb2.VoteReply(**reply)
    
    ### RUNNERS ###
    
    def run_leader(self):
        self.leader_heartbeat()
    
    def run_candidate(self):
        self.start_election()
        if len(self.votes_received) >= len(self.membership_ids) // 2 + 1:
            self.current_role = 'Leader'
            self.current_leader = self.node_id
        else:
            self.current_role = 'Follower'
        time.sleep(random())
    
    def run_follower(self):
        # TODO: Check for leader heartbeat, if None, self.current_role = 'Candidate'
        pass
    
    def run(self):
        if self.current_role == 'Leader':
            self.run_leader()
        elif self.current_role == 'Candidate':
            self.run_candidate()
        else:
            self.run_follower()
            