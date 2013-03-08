# Shortcut for connecting to OMERO servers
#
# To use this rename servers.conf.example to servers.conf in the current
# directory, and edit:
#
# #name host      port username password
# local localhost 4064 test1    test1
#
# In ipython you can run
# > import connect
# > co = connect.XXX()
# to connect, where XXX is one of the shortnames in the servers.conf

import omero
import omero.gateway
import os.path


class Connection:
    def __init__(self, user, passwd, host, port, keepAlive):
        self.user = user
        self.passwd = passwd
        self.host = host
        self.port = port
        self.keepAlive = keepAlive
        self.cli = None
        self.c()

    def c(self):
        self.close()
        self.cli = omero.client(host=self.host, port=self.port)
        self.sess = self.cli.createSession(self.user, self.passwd)
        self.cli.enableKeepAlive(self.keepAlive)
        self.conn = omero.gateway.BlitzGateway(client_obj=self.cli)
        self.res = self.sess.sharedResources()

    def close(self):
        if self.cli:
            self.cli.closeSession()


def _readServers(file):
    cfg = {}
    with open(file) as f:
        try:
            for line in f:
                line = line.strip()
                if not line or line.startswith('#'):
                    continue
                v = line.split()
                if len(v) != 5:
                    raise Exception('Invalid line: "%s"' % line)
                print v
                cfg[v[0]] = { 'host': v[1], 'port': int(v[2]),
                              'user': v[3], 'passwd': v[4]     }
        except IOError:
            pass

    return cfg


def _makeFunction(v):
    def f(user=v['user'], passwd=v['passwd'], host=v['host'], port=v['port'],
          keepAlive=60):
        return Connection(user, passwd, host, port, keepAlive)
    return f

# Read servers.conf in the directory containing this script
_cfg = _readServers(os.path.join(os.path.dirname(os.path.realpath(__file__)),
                                 'servers.conf'))
for _k in _cfg:
    vars()[_k] = _makeFunction(_cfg[_k])
