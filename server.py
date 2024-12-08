import grpc
import keyvaluestore_pb2 as kv_pb2
import keyvaluestore_pb2_grpc as kv_pb2_grpc
from concurrent import futures

class LogEntry:
    def __init__(self, key, value, term):
        self.key = key
        self.value = value
        self.term = term

class ServerHandler(kv_pb2.KeyValueStoreServicer):
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
        self.initialize_connections()

    def initialize_connections(self):
        # TODO: Set up gRPC connections to membership servers
        pass

    # Invoked by: Follower (who suspects Leader fail), Candidate
    def start_election(self):
        self.current_term += 1
        self.current_role = 'Candidate'
        self.voted_for = self.node_id
        self.votes_received = {self.node_id}
        log_term = self.log[-1].term if len(self.log) else 0 
        
        # TODO: Broadcast VoteRequest to members

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
            # TODO: Send VoteResponseRPC(true) to candidate_id
            return True
        else:
            # TODO: Send VoteResponseRPC(false) to candidate_id
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
                    # TODO: self.replicate_log(self.node_id, follower_id)
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
        # TODO: send LogRequest(leader_id, current_term, prefix_length, prefix_term, commit_length, suffix)
    
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
                # TODO: LogResponse(self.node_id, self.current_term, new_acked_length, True) to leader_id
                return new_acked_length, True
            else:
                # TODO: LogResponse(self.node_id, self.current_term, 0, False) to leader_id
                return 0, False
            
    # Invoked by: Leader
    def on_log_response(self, follower_id, term, new_acked_length, success):
        if term == self.current_term and self.current_role == 'Leader':
            if success and new_acked_length >= self.acked_length[follower_id]:
                self.sent_length[follower_id] = new_acked_length
                self.acked_length[follower_id] = new_acked_length
                # TODO: self.commit_log_entries()
            elif self.sent_length[follower_id] > 0:
                self.sent_length[follower_id] -= 1
                # TODO: self.replicate_log(self.node_id, follower_id)
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
    def on_put_request(self, key, value):
        if self.current_role == 'Leader':
            self.log.append(LogEntry(key, value, self.current_term))
            self.acked_length[self.node_id] = len(self.log)
            # TODO: self.replicate_log(self.node_id, follower_id) for every follower
        else:
            # TODO: Forward request to self.current_leader
            pass
    
    # Invoked by: Leader
    def leader_heartbeat(self):
        if self.current_role == 'Leader':
            # TODO: self.replicate_log(self.node_id, follower_id) for every follower
            pass
        
    # GRPC IMPLEMENTATIONS
    
    def GetState(self, request, context):
        reply = {"term": self.current_term, "isLeader": self.current_leader == self.node_id}
        return kv_pb2.State(**reply)

    def Get(self, request, context):
        pass

    def Put(self, request, context):
        pass
        # If Leader, append to log, return True

    def Replace(self, request, context):
        pass

    def AppendEntries(self, request, context):
        new_acked_length, success = self.on_log_request(request.leaderId, request.term, request.prefixLength, request.prefixTerm, request.leaderCommit, request.entries)
        reply = {"followerId": self.node_id, "term": self.current_term, "newAckedLength": new_acked_length, "success": success}
        return kv_pb2.AppendEntriesReply(**reply)

    def RequestVote(self, request, context):
        vote_granted = self.on_vote_request(request.term, request.candidateId, request.lastLogIndex + 1, request.lastLogTerm)
        reply = {"term": self.current_term, "voteGranted": vote_granted}
        return kv_pb2.VoteReply(**reply)
    
    # TODO: run_candidate -- send out VoteRequest and process the vote responses using on_vote_response
            