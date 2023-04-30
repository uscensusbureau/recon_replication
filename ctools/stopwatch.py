#As a work of the United States government, this project is in the public
#domain within the United States. Additionally, we waive copyright and related
#rights in the work worldwide through the CC0 1.0 Universal public domain
#dedication (https://creativecommons.org/publicdomain/zero/1.0/)

import time

class stopwatch:
    def __init__(self) -> None:
        self.running = False

    def start(self):
        self.t0 = time.time()
        self.running = True
        return self

    def stop(self):
        self.t1 = time.time()
        self.running = False
        return self

    def elapsed(self) -> float:
        if self.running:
            return time.time() - self.t0
        else:
            return self.t1 - self.t0


