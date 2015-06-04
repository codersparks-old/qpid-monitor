from qmf.console import Session, Console
import logging
from time import sleep, ctime

class QpidMonitor(Console):
	logger = logging.getLogger("qpid-monitor")
	queueMap = {}
	exchangeMap = {}
	queueInterest = []
	exchangeInterest = []
	
	def add_queue_interest(self, queue_name):
		self.logger.debug("Adding interest in queue: %s" % queue_name) 
		self.queueInterest.append(queue_name)
		
	def add_exchange_interest(self, exchange_name):
		self.logger.debug("Adding interest in exchange: %s" % exchange_name)
		self.exchangeInterest.append(exchange_name)
		
	def objectProps(self, broker, record):
		
		# Verify that we have received a queue or exchange object. Exit otherwise
		classKey = record.getClassKey()
		self.logger.debug("Processing Property, class name: %s" % classKey.getClassName())
		if classKey.getClassName() == "queue":
			name = record.name
			if name not in self.queueInterest:
				self.logger.debug("Discarding record as queue %s not registered for queue interest" % name)
				return
			
			oid = record.getObjectId()
			if oid not in self.queueMap:
				self.logger.debug("Updating queueMap for queue id: %s, name %s" % (oid, name))
				self.queueMap[oid] = name
			
		elif classKey.getClassName() == "exchange":
			name = record.name
			if name not in self.exchangeInterest:
				self.logger.debug("Discarding record as exchange %s not registered for exchange interest" % name)
				return
				
			oid = record.getObjectId()
			if oid not in self.exchangeMap:
				self.logger.debug("Updating exchangeMap for exchange id: %s, name %s" % (oid, name))
				self.exchangeMap[oid] = name
		else:
			self.logger.debug("Not processing properties for class name: %s" % classKey.getClassName())
	
	def objectStats(self, broker, record):
		''' Function handles statistic updates from the qmf framework '''
		
		timestamp = ctime()
		
		# We are only interested in those queues we have registed into hte queueMap via
		# the objectProps function if the object id is not a key of the map then we simply return
		oid = record.getObjectId()
		if oid in self.queueMap:
			
			# we get the queue name from the queueMap
			queue_name=self.queueMap[oid]
			
			self.logger.debug("Processing queue stats, id: %s, name: %s" % (oid, queue_name))
			# We then use the handle_queue_record function to process the record
			self.handle_queue_record(queue_name, record, timestamp)
			
			# if the delete-time is non-zero, this object has been deleted.  Remove it from the map.
			if record.getTimestamps()[2] > 0:
				self.logger.debug("Delete time is non zero therefore it has been deleted, removing from map")
				queueMap.pop(oid)
			
		elif oid in self.exchangeMap:
		
			# We get the exchange name from the exchange map
			exchange_name = self.exchangeMap[oid]
			
			self.logger.debug("Processing exchange stats, id: %s, name: %s" % (oid, exchange_name))
			
			# We then hange the record in the handle_exchange function
			self.handle_exchange_record(exchange_name, record, timestamp)
			
			# if the delete-time is non-zero, this object has been deleted.  Remove it from the map.
			if record.getTimestamps()[2] > 0:
				self.logger.debug("Delete time is non zero therefore it has been deleted, removing from map")
				exchangeMap.pop(oid)
	
	def handle_queue_record(self, name, record, timestamp):
		
		print("%s: Hadling record from queue %s, details %s" % (timestamp, name, vars(record)))
	
	def handle_exchange_record(self, name, record, timestamp):
		print("%s: Handling record from exchange %s, details %s" % (timestamp, name, vars(record)))
		
	def stop_monitoring(self):
		self.monitor = False
		
	def monitor_qpid(self):
		
		self.logger.debug("Staring monitoring...")
		
		# Create an instance of the QMF session manager.  Set userBindings to True to allow
		# this program to choose which objects classes it is interested in.
		sess = Session(self, manageConnections=True, rcvEvents=False, userBindings=True)
		
		# Register to receive updates for broker:queue objects.
		sess.bindClass("org.apache.qpid.broker", "queue")
		sess.bindClass("org.apache.qpid.broker", "exchange")
		broker = sess.addBroker()
		
		self.monitor = True
		while(self.monitor):
			sleep(1)
		
		self.logger.debug("monitor set to false, therefore stopping monitor")
		# Disconnect the broker before exiting
		sess.delBroker(broker)
		sess = None
