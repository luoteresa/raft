# import grpc

class Server:
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
        # Set up gRPC connections to membership servers
        # number_of_servers = len(self.membership_ids) + 1
        # start_idx = 7001 + self.node_id * number_of_servers
        # for i in range(number_of_servers):
        #     if i != self.node_id:
        #         port_number = 9001 + i
        #         src_port = start_idx + i
        #         channel = grpc.insecure_channel(f"127.0.0.1:{port_number}", 
        #                                         options=[('grpc.lb_policy_name', 'pick_first')])
        #         self.raft_peers[i] = channel
        pass

    def start_election(self):
        self.current_term += 1
        self.current_role = 'Candidate'
        self.voted_for = self.node_id
        self.votes_received = {self.node_id}
        log_term = self.log[-1].term if len(self.log) else 0 
        
        # TODO: Broadcast VoteRequest to members

    # Invoked by: Follower, Candidate, Leader
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
            # TODO: Send VoteResponseRPC(true)
        else:
            # TODO: Send VoteResponseRPC(false)
            pass
        
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
                    # TODO: ReplicateLog(node_id, follower_id)
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
    
    def on_log_request(leader_id, term, prefix_length, prefix_term, leader_commit, entries):
        pass