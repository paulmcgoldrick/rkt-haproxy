from json import loads
from zope.interface import implements

from twisted.internet.defer import succeed, Deferred
from twisted.internet.protocol import Protocol
from twisted.web.iweb import IBodyProducer
from twisted.internet import reactor
from twisted.web._newclient import ResponseDone
from twisted.web.client import Agent
from twisted.web.http_headers import Headers
import urllib


class ETCDError(Exception):
    """
    I represent an error within ETCD
    not internal errors
    """
    def __init__(self, msg, error_code, json):
        super(ETCDError, self).__init__(msg)
        self.error_code = error_code
        self.msg = msg
        self.json = json


class ETCDValue(object):
    """
    I am a key/value pair
    """
    def __init__(self, key, value, json):
        self.key = key
        self.value = value
        self.json = json

    def __repr__(self):
        return {"key": self.key, "value": self.value,
                "json": self.json}.__repr__()


class ETCDClientProtocol(Protocol):
    def __init__(self, finished):
        self.finished = finished
        self.data = ""
        self.json = None

    def dataReceived(self, bytes):
        self.data += bytes

    def connectionLost(self, reason):
        if reason.type != ResponseDone:
            self.finished.callback(reason)
        else:
            self.finished.callback(self.data)


class StringProducer(object):
    implements(IBodyProducer)

    def __init__(self, body):
        self.body = body
        self.length = len(body)

    def startProducing(self, consumer):
        consumer.write(self.body)
        return succeed(None)

    def pauseProducing(self):
        pass

    def stopProducing(self):
        pass


class Client(object):
    """ I implement v2 of the etcd specification """

    def __init__(self, etcd_host, appname, protocol="http://"):
        self.api_version = "v2"
        self.appname = appname
        self.protocol = protocol
        self.etcd_host = etcd_host
        self.agent = Agent(reactor)

    def _request(self, method, key, params=None):
        """ I am a request wrapper for Agent.request.
            method should be one of these  string representations
            of an HTTP Method: GET, PUT, DELETE """
        url = "{}{}/{}/keys/{}".format(self.protocol, self.etcd_host,
                                       self.api_version, key)

        if params:
            params = urllib.urlencode(params)
            body = StringProducer(params)
        else:
            body = None
        headers = Headers({
            'User-Agent': ['Twisted Web Client'],
            'Accept-Encoding': ['identity'],
            'Content-Type': ['application/x-www-form-urlencoded']})
        print """requesting {} @ {}
        with {} headers, and {} body""".format(method, url, headers, body)
        d = self.agent.request(method, url, headers, body)
        d.addCallback(self.cbCollectBody)
        d.addCallback(self.cbParseBody)
        d.addErrback(self.ebLog)
        return d

    def ebLog(self, error):
        """
        Way back
        """
        print "ERROR", error
        return error

    def cbCollectBody(self, response):
        """
        All the bodies
        """
        finished = Deferred()
        finished.addErrback(self.ebLog)
        response.deliverBody(ETCDClientProtocol(finished))
        return finished

    def cbParseBody(self, data):
        """
        I parse the json returned from the api call and
        and create an ETCDError or ETCDValue object from it
        """
        print data
        dd = loads(data)
        if "errorCode" in dd:
            res = ETCDError(dd["message"], dd["errorCode"], data)
            raise res
        elif dd["action"] == "delete":
            # we dont' want that slash it'll just confuse folks
            res = ETCDValue(dd['node']['key'][1:], None, data)
        elif "node" in dd and "dir" in dd["node"]:
            res = ETCDValue(dd["node"]["key"][1:], "dir", data)
        else:
            res = ETCDValue(dd['node']['key'][1:], dd['node']['value'], data)
        return res

    def get(self, key, wait=False):
        """
        I retrieve values from the configured etcd2 cluster
        """
        method = "GET"
        if not wait:
            params = None
        else:
            params = {"wait": "true"}
        return self._request(method, key, params=params)

    def set(self, key, value):
        """ I set values in the etcd cluster """
        method = "PUT"
        params = {"value": value}
        return self._request(method, key, params=params)

    def update(self, key, value):
        """ I update existing values in the etcd cluster """
        method = "PUT"
        params = {"value": value}
        return self._request(method, key, params=params)

    def rm(self, key):
        """ I remove keys from etcd """
        method = "DELETE"
        return self._request(method, key)


def add_call(d, method, key=None, value=None):
    """ I add another deferred method to this callback
    chain"""
    print d
    if key:
        if value:
            d = method(key, value)
        else:
            d = method(key)
    else:
        d = method()
    return d


def print_exeption(x):
    print x
    return x

if __name__ == "__main__":
    print "instantiating!"
    client = Client("10.0.0.14:2379", "derp")
    d = client.set("herp", "derp")
    d.addCallback(add_call, client.get, "herp")
    d.addCallback(add_call, client.update, "herp", "derp")
    d.addCallback(add_call, client.rm, "herp")
    d.addErrback(print_exeption)
    x = client.get("herp", wait=True)
    x.addBoth(print_exeption)
    reactor.run()
