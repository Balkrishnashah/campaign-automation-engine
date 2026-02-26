import uuid

class CommunicationIDGenerator:
    
    
    def generateId(self, n_rows):
        base = str(uuid.uuid4())
        return [f"{base}-{i+1}" for i in range(n_rows)]
