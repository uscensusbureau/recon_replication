#As a work of the United States government, this project is in the public
#domain within the United States. Additionally, we waive copyright and related
#rights in the work worldwide through the CC0 1.0 Universal public domain
#dedication (https://creativecommons.org/publicdomain/zero/1.0/)

import sys
import time


def print_stderr(s: str) -> None:
    print(s, file=sys.stderr)

class Timer:
    def __init__(self, message: str = 'Elapsed time:', notifier=print_stderr) -> None:
        self.notifier = notifier
        if '%' in message:
            self.message = message
        else:
            self.message = message + " %f seconds"

    def __enter__(self):
        self.start = time.time()
        return self

    def __exit__(self, *args) -> None:
        self.end = time.time()
        self.interval = self.end - self.start
        if (self.message is not None) and (self.notifier is not None):
            self.notifier(self.message % self.interval)
