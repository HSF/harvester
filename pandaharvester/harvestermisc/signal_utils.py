import os
import signal


# signal handler for suicide
def suicide_handler(sig, frame):
    if os.getppid() == 1:
        os.killpg(os.getpgrp(), signal.SIGKILL)
    else:
        os.kill(os.getpid(), signal.SIGKILL)


# set suicide handler
def set_suicide_handler(signal_type=signal.SIGTERM):
    if signal_type is not None:
        signal.signal(signal_type, suicide_handler)
    else:
        signal.signal(signal.SIGINT, suicide_handler)
        signal.signal(signal.SIGHUP, suicide_handler)
        signal.signal(signal.SIGTERM, suicide_handler)
        signal.signal(signal.SIGALRM, suicide_handler)
