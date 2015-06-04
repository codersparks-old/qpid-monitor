from qpid_monitor import *
import logging
import threading
import signal
import sys
import time

logging.basicConfig(level=logging.ERROR)
qpid_logger = logging.getLogger("qpid")
qpid_logger.setLevel(logging.ERROR)
logger = logging.getLogger("Monitor-Main")

monitoring = True

def signal_handler(signal, frame):
	logger.error("CTRL_C Caught terminating")
	# need a better way of terminating as we need to close the sessions
	sys.exit(0)	

if __name__ == "__main__":
	
	qpidMonitor = QpidMonitor()
	signal.signal(signal.SIGINT, signal_handler)
	
	
	qpidMonitor.add_queue_interest("hello-world")
	qpidMonitor.add_exchange_interest("")
	qpidMonitor.add_exchange_interest("amq.direct")
	
	qpid_thread = threading.Thread(target=qpidMonitor.monitor_qpid)
	qpid_thread.daemon = True
	qpid_thread.start()
	
	while(monitoring):
		time.sleep(1)
	
	
	
	
