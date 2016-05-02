from twisted.internet import protocol
from twisted.internet import reactor


class ProcProtocol(protocol.ProcessProtocol):

    def connectionMade(self):
        print "connectionMade!"

    def outReceived(self, data):
        print "outReceived! with %d bytes!" % len(data)

    def errReceived(self, data):
        print "errReceived! with %d bytes!" % len(data)
        print data

    def inConnectionLost(self):
        print "inConnectionLost! stdin is closed! (we probably did it)"

    def outConnectionLost(self):
        print "outConnectionLost! The child closed their stdout!"

    def errConnectionLost(self):
        print "errConnectionLost! The child closed their stderr."

    def processExited(self, reason):
        print "processExited, status %d" % (reason.value.exitCode,)
        print reason

    def processEnded(self, reason):
        print "processEnded, status %d" % (reason.value.exitCode,)
        print "quitting"

    def childConnectionLost(self, reason):
        print "processEnded", reason


class Process(object, ProcProtocol):
    """
    I am a process that's being managed by container kit
    """
    def __init__(self, name, updateCallback=None):
        self.name = name
        self._proc_handle = None

    def __getitem__(self, key):
        """
        lets act like a dict for the sake of easy
        attribute access
        """
        return getattr(self, key)

    def issueCommand(self, command, new_process=True):
        print "COMMAND IS ", command
        self._proc_handle = reactor.spawnProcess(self, command.split()[0],
                                                 command.split(), {})
        return self._proc_handle

    def sendSignal(self, signal):
        pass

    @property
    def pid(self):
        return self._proc_handle.pid

    @property
    def status(self):
        return self._proc_handle.status
