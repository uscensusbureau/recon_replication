#As a work of the United States government, this project is in the public
#domain within the United States. Additionally, we waive copyright and related
#rights in the work worldwide through the CC0 1.0 Universal public domain
#dedication (https://creativecommons.org/publicdomain/zero/1.0/)

"""
Class to watch the calling thread and report what it's doing.

https://stackoverflow.com/questions/45290223/check-what-thread-is-currently-doing-in-python


"""

import threading
import sys
import time

class StackSurveillance:
    def __init__(self, thread=threading.current_thread(), file=sys.stderr, interval: float = 60.0, callback=None) -> None:
        self.thread = thread
        self.file   = file
        self.callback=callback
        self.t      = threading.Thread(target=self.run)
        self.t.start()

    def run(self) -> None:
        while True:
            frame = sys._current_frames().get(self.thread.ident, None)
            if frame:
                print(frame, file=self.file)
                print(frame.f_code.co_filename, frame.f_code.co_name, frame.f_code.co_firstlineno, file=self.file)
                if self.callback is not None:
                    self.callback(frame)


if __name__=="__main__":
    print("demo program to show use with a slow fibinacci program.")

    def slow_fib(a) -> int:
        time.sleep(1)
        if a>2:
            return slow_fib(a -1) +slow_fib(a -2)
        return 1
