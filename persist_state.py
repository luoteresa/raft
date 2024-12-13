import pickle
import os
from threading import Lock

LOG_FILE = 'node.pkl' # File to store persisted states
INDEX_FILE = 'index.log' # File to track last saved index
BUFFER_SIZE = 10 # Commit every 10 states

class PersistentStateStore:
    def __init__(self, log_fn, index_fn):
        self.lock = Lock()
        self.buffer = []  # In memory buffer
        self.log_fn = log_fn
        self.index_fn = index_fn
        self.last_length = self.load_last_length()
        self.buffer_size = 50
        
        

    def save_state(self, state):
        self.buffer.append(state)
        
        if len(self.buffer) >= BUFFER_SIZE:
            self._flush_to_disk()
    
    def _flush_to_disk(self):
        with open(self.log_fn, 'ab') as f:
            for state in self.buffer:
                pickle.dump(state, f)
        
        self.last_length += len(self.buffer)
        self._update_last_index()
        
        self.buffer = []

    def _update_last_index(self):
        with open(self.index_fn, 'w') as index_file:
            index_file.write(str(self.last_length))

    def load_last_length(self):
        if os.path.exists(self.index_fn):
            with open(self.index_fn, 'r') as index_file:
                return int(index_file.read())
        return 0 

    def load_all_states(self):
        states = []
        if os.path.exists(self.log_fn):
            with open(self.log_fn, 'rb') as f:
                while True:
                    try:
                        states.append(pickle.load(f))
                    except EOFError:
                        break
        return states

    def close(self):
        if self.buffer:
            self._flush_to_disk()
