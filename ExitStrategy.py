"""
@author: Keyon Genesis
"""
import signal


class ExitStrategy(object):
    """
    intercepts kill signal and provide flag if exit is requested
    """
    kill_now = False

    def __init__(self):
        signal.signal(signal.SIGINT, self.exit_gracefully)
        signal.signal(signal.SIGTERM, self.exit_gracefully)

    def exit_gracefully(self,signum, frame):
        self.kill_now = True

