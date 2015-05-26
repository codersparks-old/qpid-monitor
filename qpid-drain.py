'''
Created on 26 May 2015

@author: sparks
'''

import optparse
from qpid.messaging import *
from qpid.util import URL
from qpid.log import enable, DEBUG, WARN

parser = optparse.OptionParser(usage="usage: %prog [options] ADDRESS ...",
                               description="Drain messages from the supplied address.")
parser.add_option("-b", "--broker", default="localhost",
                  help="connect to specified BROKER (default %default)")
parser.add_option("-c", "--count", type="int",
                  help="number of messages to drain")
parser.add_option("-f", "--forever", action="store_true",
                  help="ignore timeout and wait forever")
parser.add_option("-r", "--reconnect", action="store_true",
                  help="enable auto reconnect")
parser.add_option("-i", "--reconnect-interval", type="float", default=3,
                  help="interval between reconnect attempts")
parser.add_option("-l", "--reconnect-limit", type="int",
                  help="maximum number of reconnect attempts")
parser.add_option("-t", "--timeout", type="float", default=0,
                  help="timeout in seconds to wait before exiting (default %default)")
parser.add_option("-p", "--print", dest="format", default="%(M)s",
                  help="format string for printing messages (default %default)")
parser.add_option("-v", dest="verbose", action="store_true",
                  help="enable logging")

opts, args = parser.parse_args()

if opts.verbose:
  enable("qpid", DEBUG)
else:
  enable("qpid", WARN)

if args:
  addr = args.pop(0)
else:
  parser.error("address is required")
if opts.forever:
  timeout = None
else:
  timeout = opts.timeout

class Formatter:

  def __init__(self, message):
    self.message = message
    self.environ = {"M": self.message,
                    "P": self.message.properties,
                    "C": self.message.content}

  def __getitem__(self, st):
    return eval(st, self.environ)

conn = Connection(opts.broker,
                  reconnect=opts.reconnect,
                  reconnect_interval=opts.reconnect_interval,
                  reconnect_limit=opts.reconnect_limit)
try:
  conn.open()
  ssn = conn.session()
  rcv = ssn.receiver(addr)

  count = 0
  while not opts.count or count < opts.count:
    try:
      msg = rcv.fetch(timeout=timeout)
      print opts.format % Formatter(msg)
      count += 1
      ssn.acknowledge()
    except Empty:
      break
except ReceiverError, e:
  print e
except KeyboardInterrupt:
  pass

conn.close()
