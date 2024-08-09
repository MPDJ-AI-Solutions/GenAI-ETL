import time

class Stopwatch:
    def __init__(self):
        self.start = time.time()

    def restart(self):
        self.time = time.time()
    
    def end(self):
        return time.time() - self.start


