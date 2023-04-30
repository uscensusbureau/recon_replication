#As a work of the United States government, this project is in the public
#domain within the United States. Additionally, we waive copyright and related
#rights in the work worldwide through the CC0 1.0 Universal public domain
#dedication (https://creativecommons.org/publicdomain/zero/1.0/)

"""notify.py: notify using SNS or some other approach"""

import sys
import os
import copy
from subprocess import run, Popen, PIPE


def notify(msg: str) -> None:
    if 'DAS_SNSTOPIC' in os.environ:
        old_env = copy.deepcopy(os.environ)
        if 'BCC_PROXY' in os.environ:
            os.environ['HTTP_PROXY'] = os.environ['HTTPS_PROXY'] = os.environ['BCC_PROXY']
            ret = Popen(['aws', 'sns', 'publish', '--topic-arn', os.environ['DAS_SNSTOPIC'], '--message', msg], stdout=PIPE).communicate()[0]

        for var in ['HTTP_PROXY', 'HTTPS_PROXY']:
            if var not in old_env:
                del os.environ[var]
            else:
                os.environ[var] = old_env[var]


if __name__=="__main__":
    msg = " ".join(sys.argv[1:])
    notify(msg)
