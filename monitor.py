from qpid-monitor import *
import logging
import threading
import signal
import sys

logger = logging.logger("Monitor-Main")

def signal_handler(signal, frame):
	logger.debug("Handling ctrl-c...")
	qpidMonitor.stop_monitoring()

if __name__ == "__main__":
	qpidMonitor = QpidMonitor()
	
	
	
	qpidMonitor.add_queue_interest("hello-world")
	qpidMonitor.add_exchange_interest("")
	qpidMonitor.add_exchange_interest("amq.direct")
	
	qpid-thread = threading.Thread(target=qpidMonitor.monitor_qpid)
	qpid-thread.start()
	
	
	
	qpid-thread.join()
	
	
	