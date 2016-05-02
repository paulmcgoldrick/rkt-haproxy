import yaml
from json import loads
from twisted.internet.defer import inlineCallbacks, returnValue
from twisted.internet.defer import DeferredList, Deferred
from twisted.internet import reactor, task

from etcd import Client
from process import Process
from templates import ConfigurationTemplate
from templates import CommandTemplate
from helpers import get_interfaces


class ContainerKit(object):
    """
    I am a container wrapper that allows for
    dynamic reloading of configs, the waiting for
    keys before starting a service, and the loading
    of configuration data from multiple sources.
    """

    def __init__(self, config="/ck_conf.yml"):
        """
        Initialize the python object. I take a path
        to my yaml config file as an option.
        """
        self.config = config
        self.id = None
        self.name = None
        self.template = None
        self.dest = None
        self.client = None
        self.directory = None
        self.proc = None
        # Lets kick it off
        reactor.callLater(1, self.initializeProcess)

    def __getitem__(self, key):
        """
        allow use to be subscriptable
        """
        return self.__dict__[key]

    def initializeProcess(self):
        """
        Initialize all the objects and services
        for this ContainerKit implementation
        """
        with open(self.config) as conf:
            conf = yaml.load(conf)
        self.name = conf["app"]["name"]
        self.template = ConfigurationTemplate(conf["app"]["template"])
        self.dest = conf["app"]["dest"]
        self.client = Client(conf["etcd_host"], conf["app"]["name"])
        self.start_template = CommandTemplate(conf['app']['start'])
        self.restart_template = CommandTemplate(conf['app']['restart'])
        self.stop_template = CommandTemplate(conf['app']['stop'])
        self.proc = Process(self.name, self.cbProcessUpdated)
        self.vars = conf['app']['vars']
        self.publish_vars = conf['app']['publish_vars']
        self.vars.update(get_interfaces())
        print self.vars
        self.publish_vars = conf['app']['publish_vars']
        self.wait_keys = conf['app']['wait_keys']
        self.directory = "containerkit/"+self.proc.name+"/"

        # Start callback flow
        d = self.registerWithCluster()
        d.addCallback(lambda ignored: self.servicectl("start"))
        d.addCallback(lambda ignored: self.publishVars())

    def processWaitKeys(self):
        """
        Iterate through all the wait keys and process them
        Ensure these keys exist, or match before starting the
        service that I'm wrapping
        """
        tasks = []
        for key in self.wait_keys["exists"]:
            finished = Deferred()
            d = self.client.get("containerkit/{}".format(key))
            d.addCallback(lambda ignored: finished.callback(None))
            d.addErrback(self.continueWaiting, key, finished)
            tasks.append(finished)
        task_lst = DeferredList(tasks)
        return task_lst

    def continueWaiting(self, d, key, finished):
        """
        I am a callback that acts as a scheduling loop
        to poll for our required keys
        """
        d = task.deferLater(reactor,
                            10, self.client.get,
                            "containerkit/{}".format(key))
        d.addErrback(self.continueWaiting, key, finished)
        d.addCallback(lambda ignored: finished.callback(None))
        return d

    def registerWithCluster(self):
        """
        I announce this instance of containerkit with
        the etcd cluster, and advertise my application
        """
        d = self.client.get(self.directory)
        d.addCallback(self.cbAssignID)
        d.addErrback(lambda ignored: self.client.set(
            self.directory+"0/state", "present"))
        return d

    def cbAssignID(self, request):
        """
        Extract hte json result for the directory request
        and determine the next available ID for this application
        assign it locally and push the property to etcd
        """
        
        nodes = loads(request.json)["node"]["nodes"]
        print "NODES ARE ", nodes
        # I am not proud (I am proud(just a little))
        l = int(sorted([int(i['key'].split('/')[-1]) for i in nodes
                        if i['key'][-1].isdigit()])[-1]) + 1
        print l
        self.client.set("{}{}/state".format(self.directory, l), "present")
        self.id = l
        return request

    def publishVars(self):
        """
        Publish the variables listed in the config for
        publishing
        """
        for k, v in self.publish_vars.iteritems():
            v = CommandTemplate(v)
            d = self.processTemplate(v)
            d.addCallback(self.cbRenderTemplate)
            d.addCallback(lambda v: self.pushProp(k, v))
        return d

    def pushProp(self, prop_name, property_value, appname=None):
        """
        I add a Key/Value entry to etcd. The default behavior is
        to place this key under our /containerkit/appname/ directory
        but optionally I take an appname argument which allows for
        modification of this behavior. Use self.client.set if you
        would like more freedom of key names
        """
        if not appname:
            d = self.client.set(
                "{}{}/{}".format(self.directory, self.id, prop_name),
                property_value)
        else:
            d = self.client.set(
                "{}/{}".format(self.directory, prop_name),
                property_value)
        return d

    def servicectl(self, action):
        """
        I mimic an init system in that I allow control over
        our underlying service. The action argument can be a
        string currently "start", "restart", or "stop"
        """
        # wait for all the keys we need to appear
        d = self.processWaitKeys()
        tmpl = getattr(self, "{}_template".format(action))
        items = []
        # render the command template and the
        # configuration template
        for tmp in [self.template, tmpl]:
            template = Deferred()
            items.append(template)
            print "ADDING processTemplate"
            template.addCallback(self.processTemplate)
            print "ADDING cbRenderTemplate"
            template.addCallback(self.cbRenderTemplate)
            if isinstance(tmp, CommandTemplate):
                print "ADDING issueCommand"
                template.addCallback(self.proc.issueCommand)
            elif isinstance(tmp, ConfigurationTemplate):
                print "ADDING writeConfig"
                template.addCallback(self.writeConfig)
            template.tmp = tmp
        print "items is ", items
        ditems = DeferredList(items)
        d.addCallback(self.cbProcessServices, items)
        return ditems

    def cbProcessServices(self, wait_keys, templates):
        for i in templates:
            i.callback(i.tmp)
        return wait_keys

    def writeConfig(self, config):
        print "in writeConfig with ", config
        with open(self.dest, "wb") as fh:
            fh.write(config)
        return config

    @inlineCallbacks
    def processTemplate(self, template):
        """
        I take a CommandTemplate, or a ConfigurationTemplate
        instance as an argument and infer from it the variables
        required to render the template. I then search
        for this variables and place them in a "var" property
        of the template. Currently search order is. self.vars
        self, self.proc, and etcd
        """
        template.vars = {}
        # We've just been provided a signal
        # for this command template
        print "In processTemplate"
        if isinstance(template, int):
            returnValue(template)
        tmpl_vars = {}
        var_list = []

        for var in template.needed:
            var = var.strip()
            print "LOOKING FOR ", var
            d = Deferred()
            d.addCallback(lambda ignored: self.vars[var])
            d.addErrback(lambda ignored: self[var])
            d.addErrback(lambda ignored: getattr(self.proc, var))
            d.addErrback(self.cbLookupRemoteVar,
                            "{}{}/{}".format(
                             self.directory, self.id, var))
            d.addErrback(self.cbLookupRemoteVar,
                           "containerkit/{}".format(var))
            d.addErrback(self.cbLookupRemoteVar,"{}".format(var))
            d.addCallback(lambda ignored,var: tmpl_vars.update({var: ignored}), var)
            var_list.append(d)
            d.callback(None)

        var_list = DeferredList(var_list)
        var_list = yield var_list
        print "TMPL_VARS is ", tmpl_vars
        template.vars = tmpl_vars
        returnValue(template)

    def cbLookupRemoteVar(self, d, path):
        """
        I'm a simple wrapper around self.client.get
        TODO: lambda me
        """
        path = path.replace("__", "/")
        d = self.client.get(path)
        d.addCallback(lambda d: getattr(d, "value"))
        return d

    def cbRenderTemplate(self, template):
        """
        I'm a simple wrapper around template.render
        TODO: lambda me
        """
        return template.render(template.vars)

    def cbProcessUpdated(self, process):
        """
        I'm a callback thats passed to the underlying Process object
        that allows that process to notify us if there's been a change
        in its status
        """
        d = Deferred()
        d.addCallback(self.cbPushStatus)
        d.callback(process)

    def cbPushStatus(self, process):
        """
        Schedule a status update to etcd
        """
        d = self.client.set(
            "{}{}/status".format(self.directory, self.id), process.status)
        return d


if __name__ == "__main__":
    # pp = Process(
    # 'rkt-haproxy', '/usr/sbin/haproxy -f /etc/haproxy/haproxy.cfg',
    # '/usr/sbin/haproxy -f /etc/haproxy/haproxy.cfg -fs {{pid}}',
    # '15'
    #        )
    # pp.start()
    ck = ContainerKit()
    # reactor.callLater(10, ck.servicectl, 'restart')
    # reactor.callLater(20, ck.servicectl, 'restart')
    reactor.run()
