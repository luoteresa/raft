import pickle
import os
from threading import Lock
# from server import LogEntry

LOG_FILE = 'node.pkl'  # File to store persisted states
INDEX_FILE = 'index.log'  # File to track last saved index
BUFFER_SIZE = 10          # Commit every 10 states

class PersistentStateStore:
    def __init__(self, log_fn, index_fn):
        self.lock = Lock()
        self.buffer = []  # In-memory buffer to store states
        self.log_fn = log_fn
        self.index_fn = index_fn
        self.last_length = self.load_last_length()
        self.buffer_size = 50
        
        

    def save_state(self, state):
        """
        Add a state to the buffer and persist to disk every 10 states.
        """
        self.buffer.append(state)
        
        # Check if the buffer is full
        if len(self.buffer) >= BUFFER_SIZE:
            self._flush_to_disk()
    
    def _flush_to_disk(self):
        """
        Persist the current buffer to the file and update the index.
        """
        # Append states to the pickle file
        with open(self.log_fn, 'ab') as f:
            for state in self.buffer:
                pickle.dump(state, f)
        
        # Update the last saved index
        self.last_length += len(self.buffer)
        self._update_last_index()
        
        # Clear the buffer after saving
        self.buffer = []

    def _update_last_index(self):
        """
        Update the last index in the index file.
        """
        
        with open(self.index_fn, 'w') as index_file:
            index_file.write(str(self.last_length))

    def load_last_length(self):
        """
        Load the last saved index from the index file.
        """
        if os.path.exists(self.index_fn):
            with open(self.index_fn, 'r') as index_file:
                return int(index_file.read())
        return 0  # Default to -1 if no index is saved

    def load_all_states(self):
        """
        Load all states from the file.
        """
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
        """
        Flush remaining states in the buffer before shutting down.
        """
        if self.buffer:
            self._flush_to_disk()

# Usage
if __name__ == "__main__":
    store = PersistentStateStore(LOG_FILE, INDEX_FILE)

    # Simulate saving 25 states
    for i in range(25):
        state = {"index": i, "term": i // 5, "data": f"State {i}"}
        store.save_state(state)
        print(f"State {i} saved to buffer.")
    
    # Ensure everything is flushed on shutdown
    store.close()

    # Load all states
    states = store.load_all_states()
    print("Loaded states:", states)
