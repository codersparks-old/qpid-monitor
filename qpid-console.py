'''
Created on 26 May 2015

@author: sparks
'''

# Import needed classes
from qmf.console import Session, Console
from time        import sleep
import socket

# Declare a dictionary to map object-ids to queue names
queueMap = {}

# Declare queue names that we are interested in
queueInterest = ["hello-world", "goodbye-world"]

# Customize the Console class to receive object updates.
class MyConsole(Console):

  # Handle property updates
  def objectProps(self, broker, record):

    # Verify that we have received a queue object.  Exit otherwise.
    classKey = record.getClassKey()
    if classKey.getClassName() != "queue":
      return
    
    # If the record name is not in the queue Interest list exit
    name = record.name
    if name not in queueInterest:
        return

    # If this object has not been seen before, create a new mapping from objectID to name
    oid = record.getObjectId()
    if oid not in queueMap:
      queueMap[oid] = record.name

  # Handle statistic updates
  def objectStats(self, broker, record):
    
    # Ignore updates for objects that are not in the map
    oid = record.getObjectId()
    if oid not in queueMap:
      return
  
    # Extract the stats that we are interested in
    queue_name=queueMap[oid]
    total_enqueues = record.msgTotalEnqueues
    total_dequeues = record.msgTotalDequeues
    queue_size_count = record.msgDepth
    queue_size_bytes = record.byteDepth
    consumer_count = record.consumerCount
    consumer_high = record.consumerCountHigh
    consumer_low =  record.consumerCountLow
    binding_count = record.bindingCount
    binding_high = record.bindingCountHigh
    binding_low =  record.bindingCountLow
    # Print the queue name and some statistics
    print "queue=%s,enqueues=%d,dequeues=%d,queue_size_count=%d,queue_size_bytes=%d,consumers=%d,consumers_high=%d,consumers_low=%d, bindings=%d, bindings_high=%d, bindings_low=%d" %  (queue_name,total_enqueues,total_dequeues,queue_size_count,queue_size_bytes,consumer_count,consumer_high,consumer_low,binding_count,binding_high, binding_low)
    #try:
    host = "127.0.0.1"
    port = 12345
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((host, port))
    s.sendall("queue=%s,enqueues=%d,dequeues=%d,queue_size_count=%d,queue_size_bytes=%d,consumers=%d,consumers_high=%d,consumers_low=%d, bindings=%d, bindings_high=%d, bindings_low=%d" %  (queue_name,total_enqueues,total_dequeues,queue_size_count,queue_size_bytes,consumer_count,consumer_high,consumer_low,binding_count,binding_high, binding_low))
    #except Exception, e:
    #    print "Error %s" % (e,)
    #finally:
    s.close()
    # if the delete-time is non-zero, this object has been deleted.  Remove it from the map.
    if record.getTimestamps()[2] > 0:
      queueMap.pop(oid)

# Create an instance of the QMF session manager.  Set userBindings to True to allow
# this program to choose which objects classes it is interested in.
sess = Session(MyConsole(), manageConnections=True, rcvEvents=False, userBindings=True)

# Register to receive updates for broker:queue objects.
sess.bindClass("org.apache.qpid.broker", "queue")
broker = sess.addBroker()

# Suspend processing while the asynchronous operations proceed.
try:
  while True:
    sleep(1)
except:
  pass

# Disconnect the broker before exiting.
sess.delBroker(broker)